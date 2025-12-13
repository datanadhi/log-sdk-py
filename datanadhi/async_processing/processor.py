"""Async processor for non-blocking pipeline triggers."""

import atexit
import threading
import time
from pathlib import Path

import requests

from datanadhi.async_processing.drain_worker import DrainWorker
from datanadhi.async_processing.health import ServerHealthMonitor
from datanadhi.async_processing.queue import SafeQueue
from datanadhi.echopost import binary as echopost
from datanadhi.server import fallback as fallback_server
from datanadhi.server import primary as primary_server
from datanadhi.utils.files import store_dropped_data

# Global instances
_PROCESSORS = {}
_LOCK = threading.Lock()


def get_processor_for_directory(
    datanadhi_dir: Path, config: dict, logger
) -> "AsyncProcessor":
    """Get or create processor for this directory.

    Uses singleton pattern - one processor per datanadhi_dir.
    """
    key = str(datanadhi_dir.absolute())

    if key in _PROCESSORS:
        return _PROCESSORS[key]

    with _LOCK:
        if key not in _PROCESSORS:
            _PROCESSORS[key] = AsyncProcessor(datanadhi_dir, config, logger)
        return _PROCESSORS[key]


class AsyncProcessor:
    """Async processor with queue-based workers and overflow protection.

    Features:
    - Non-blocking submission via SafeQueue
    - Normal workers handle primary/echopost/fallback routing
    - Drain worker automatically starts at 90% capacity
    - Health monitoring with automatic recovery
    """

    def __init__(self, datanadhi_dir: Path, config: dict, logger):
        self.datanadhi_dir = Path(datanadhi_dir).absolute()
        self.api_key = config["api_key"]
        self.queue_size = config["async_queue_size"]
        self.server_host = config["server_host"]
        self.fallback_server_host = config["fallback_server_host"]
        self.worker_count = config["async_workers"]
        self.exit_timeout = config["async_exit_timeout"]
        self.echopost_disabled = config["echopost_disable"]
        self.logger = logger

        self._shutdown = threading.Event()
        self.queue = SafeQueue(maxsize=self.queue_size)
        self.workers = []

        # Initialize health monitor
        self._health_monitor = ServerHealthMonitor(logger=self.logger)

        # Initialize drain worker
        self.drain_worker = DrainWorker(
            queue=self.queue,
            fallback_server_host=self.fallback_server_host,
            api_key=self.api_key,
            send_fn=fallback_server.send,
            health_check_fn=primary_server.is_healthy,
            logger=self.logger,
            datanadhi_dir=self.datanadhi_dir,
        )

        self._start_workers()
        atexit.register(self.flush)

    def _start_workers(self):
        """Start daemon worker threads."""
        for i in range(self.worker_count):
            t = threading.Thread(
                target=self._worker_loop, name=f"datanadhi-{i}", daemon=True
            )
            t.start()
            self.workers.append(t)

    def _worker_loop(self):
        """Main worker loop: process items from queue."""
        session = requests.Session()

        try:
            while not self._shutdown.is_set():
                item = self.queue.get(timeout=1.0)

                if item is None:
                    # Queue empty, sleep briefly
                    time.sleep(0.1)
                    continue

                # Route based on primary server health
                if self._health_monitor.is_server_up(self.server_host):
                    self._send_to_primary(session, item)
                else:
                    # Primary down, use fallback strategy
                    if self.echopost_disabled:
                        if self._health_monitor.is_server_up(
                            self.fallback_server_host, is_fallback=True
                        ):
                            self._send_to_fallback_server(session, item)
                        else:
                            self.queue.writeback_batch([item])
                            time.sleep(0.01)
                    else:
                        self._send_to_echopost(item)

        except Exception as e:
            self.logger.error(
                "Worker error",
                context={"error": str(e)},
                trace_id="datanadhi-async-worker",
                _datanadhi_internal=True,
            )
        finally:
            session.close()

    def _send_to_primary(self, session: requests.Session, item: tuple):
        """Send item to primary server."""
        try:
            pipelines, payload = item
            result = primary_server.send(
                session,
                self.server_host,
                {"pipelines": pipelines, "log_data": payload},
                self.api_key,
            )

            if result["success"]:
                self.queue.task_done()
                return

            if result["is_unavailable"]:
                # Server down, requeue and mark unhealthy
                self.queue.writeback_batch([item])
                self._health_monitor.set_health_down(
                    self.server_host, health_check_fn=primary_server.is_healthy
                )
                self.logger.warning(
                    "Primary server unavailable, requeued",
                    context={"server": self.server_host},
                    trace_id="datanadhi-async-worker",
                    _datanadhi_internal=True,
                )
                return

            if result["is_failure"]:
                # Client/server error, drop item
                file_path = store_dropped_data(
                    self.datanadhi_dir, [item], "primary_failed"
                )
                self.logger.error(
                    "Primary send failed, data dropped",
                    context={
                        "status_code": result["status_code"],
                        "file": file_path,
                    },
                    trace_id="datanadhi-async-worker",
                    _datanadhi_internal=True,
                )
                self.queue.task_done()

        except Exception as e:
            self.logger.error(
                "Primary send error, dropped",
                context={"error": str(e)},
                trace_id="datanadhi-async-worker",
                _datanadhi_internal=True,
            )
            self.queue.task_done()

    def _send_to_fallback_server(self, session: requests.Session, item: tuple):
        """Send batch to fallback server."""
        try:
            # Collect batch of items
            items = [item] + self.queue.get_batch(99)  # Total 100

            # Convert to payload format
            payloads = [{"pipelines": it[0], "log_data": it[1]} for it in items]

            # Send to fallback
            result = fallback_server.send(
                session, self.fallback_server_host, payloads, self.api_key
            )

            if result["success"]:
                for _ in items:
                    self.queue.task_done()
                return

            if result["is_unavailable"]:
                # Fallback server down, requeue
                self.queue.writeback_batch(items)
                self._health_monitor.set_health_down(
                    self.fallback_server_host,
                    is_fallback=True,
                    health_check_fn=primary_server.is_healthy,
                )
                self.logger.warning(
                    "Fallback server unavailable, requeued",
                    context={
                        "server": self.fallback_server_host,
                        "batch_size": len(items),
                    },
                    trace_id="datanadhi-async-worker",
                    _datanadhi_internal=True,
                )
                return

            if result["is_failure"]:
                # Error, drop items
                file_path = store_dropped_data(
                    self.datanadhi_dir, items, "fallback_failed"
                )
                self.logger.error(
                    "Fallback send failed, data dropped",
                    context={
                        "status_code": result["status_code"],
                        "batch_size": len(items),
                        "file": file_path,
                    },
                    trace_id="datanadhi-async-worker",
                    _datanadhi_internal=True,
                )
                for _ in items:
                    self.queue.task_done()

        except Exception as e:
            self.logger.error(
                "Fallback send error",
                context={"error": str(e), "batch_size": len(items)},
                trace_id="datanadhi-async-worker",
                _datanadhi_internal=True,
            )
            # Try to mark items as done to avoid blocking
            for _ in items:
                try:
                    self.queue.task_done()
                except Exception:
                    pass

    def _send_to_echopost(self, item: tuple):
        """Send item to EchoPost via gRPC."""
        try:
            started = echopost.start_if_socket_not_exists(
                self.datanadhi_dir, self.api_key, self.server_host
            )

            if not started:
                self.echopost_disabled = True
                self.queue.writeback_batch([item])
                return

            pipelines, payload = item
            sent = echopost.send_log_over_unix_grpc(
                self.datanadhi_dir,
                pipelines=pipelines,
                payload=payload,
                api_key=self.api_key,
            )

            if sent:
                self.queue.task_done()
            else:
                # Failed, disable echopost and requeue
                self.echopost_disabled = True
                self.queue.writeback_batch([item])
                self.logger.error(
                    "Echopost send failed, requeued",
                    trace_id="datanadhi-async-worker",
                    _datanadhi_internal=True,
                )

        except Exception as e:
            self.logger.error(
                "Echopost error, requeued",
                context={"error": str(e)},
                trace_id="datanadhi-async-worker",
                _datanadhi_internal=True,
            )
            self.echopost_disabled = True
            self.queue.writeback_batch([item])

    def submit(self, pipelines: list[str], payload: dict) -> bool:
        """Submit log for async processing.

        Args:
            pipelines: List of pipeline IDs to trigger
            payload: Log data payload

        Returns:
            True if submitted successfully, False if queue is full
        """
        success = self.queue.add((pipelines, payload))

        # Check if drain worker should start
        if success:
            self.drain_worker.start_if_needed()

        return success

    def _wait_till_drain_complete(self):
        """Wait until the queue is fully drained."""
        while not self.queue.empty():
            time.sleep(0.1)

    def flush(self):
        """Wait for queue to drain (best-effort).

        Called automatically on exit via atexit.
        """
        if not self._shutdown.is_set():

            def wait_for_queue():
                self.queue.join()

            thread = threading.Thread(target=wait_for_queue)
            thread.start()
            thread.join(timeout=self.exit_timeout)
            self._shutdown.set()
