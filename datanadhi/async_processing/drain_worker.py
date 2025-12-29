"""Drain worker to handle queue overflow."""

import threading
import time

from datanadhi.utils.files import store_dropped_data


class DrainWorker:
    """Emergency worker to drain queue to fallback at 90% capacity."""

    def __init__(
        self,
        queue,
        fallback_server_host: str,
        api_key: str,
        send_fn,
        health_check_fn,
        logger,
        datanadhi_dir,
    ):
        """Initialize drain worker with queue and fallback config."""
        self.queue = queue
        self.fallback_server_host = fallback_server_host
        self.api_key = api_key
        self.send_fn = send_fn
        self.health_check_fn = health_check_fn
        self.logger = logger
        self.datanadhi_dir = datanadhi_dir

        self._worker_thread = None
        self._lock = threading.Lock()
        self._is_running = False

    def start_if_needed(self):
        """Start drain worker if queue ≥90% and not already running."""
        fill_pct = self.queue.fill_percentage()

        if fill_pct >= 0.90:
            with self._lock:
                # Check if already running
                if (
                    self._is_running
                    and self._worker_thread
                    and self._worker_thread.is_alive()
                ):
                    return False

                # Start new worker
                self._is_running = True
                self._worker_thread = threading.Thread(
                    target=self._drain_loop, daemon=True, name="datanadhi-drain-worker"
                )
                self._worker_thread.start()
                return True

        return False

    def _drain_loop(self):
        """Drain queue in batches until it reaches 10% capacity."""
        import requests

        session = requests.Session()

        self.logger.debug(
            "Drain worker started",
            context={"queue_fill": f"{self.queue.fill_percentage():.0%}"},
            trace_id="datanadhi-drain-worker",
            _datanadhi_internal=True,
        )

        try:
            while self.queue.fill_percentage() > 0.10:
                # Wait for fallback server to be healthy
                if not self._wait_for_healthy_server(session):
                    # Could not reach server, stop draining
                    self.logger.error(
                        "Drain worker stopped, fallback unreachable",
                        context={"server": self.fallback_server_host},
                        trace_id="datanadhi-drain-worker",
                        _datanadhi_internal=True,
                    )
                    break

                # Get batch of items
                items = self.queue.get_batch(100)
                if not items:
                    break

                # Convert to payload format
                payloads = [
                    {"pipelines": item[0], "log_data": item[1]} for item in items
                ]

                # Send batch to fallback server
                result = self.send_fn(
                    session, self.fallback_server_host, payloads, self.api_key
                )

                if result["success"]:
                    # Mark as done
                    for _ in items:
                        self.queue.task_done()
                    self.logger.debug(
                        "Drain worker sent batch",
                        context={"batch_size": len(items)},
                        trace_id="datanadhi-drain-worker",
                        _datanadhi_internal=True,
                    )

                elif result["is_unavailable"]:
                    # Fallback server down, write back and wait
                    self.queue.writeback_batch(items)
                    self.logger.warning(
                        "Drain worker: fallback unavailable, retrying",
                        context={"server": self.fallback_server_host},
                        trace_id="datanadhi-drain-worker",
                        _datanadhi_internal=True,
                    )
                    time.sleep(0.1)

                else:
                    # Other error, mark as done (drop items)
                    file_path = store_dropped_data(
                        self.datanadhi_dir, items, "drain_worker_failed"
                    )
                    for _ in items:
                        self.queue.task_done()
                    self.logger.error(
                        "Drain worker batch failed, data dropped",
                        context={
                            "status_code": result.get("status_code"),
                            "batch_size": len(items),
                            "file": file_path,
                        },
                        trace_id="datanadhi-drain-worker",
                        _datanadhi_internal=True,
                    )

        except Exception as e:
            self.logger.error(
                "Drain worker error",
                context={"error": str(e)},
                trace_id="datanadhi-drain-worker",
                _datanadhi_internal=True,
            )
        finally:
            session.close()
            with self._lock:
                self._is_running = False
            self.logger.debug(
                "Drain worker stopped",
                context={"queue_fill": f"{self.queue.fill_percentage():.0%}"},
                trace_id="datanadhi-drain-worker",
                _datanadhi_internal=True,
            )

    def _wait_for_healthy_server(self, session) -> bool:
        """Wait for fallback server health (10s timeout). Returns success."""
        max_attempts = 100  # 10 seconds max wait

        for _ in range(max_attempts):
            try:
                if self.health_check_fn(session, self.fallback_server_host):
                    return True
            except Exception:
                pass

            time.sleep(0.1)

        return False
