"""Server health monitoring."""

import threading
import time

import requests


class ServerHealthMonitor:
    """Monitor health of primary and fallback servers."""

    def __init__(self, logger=None):
        self.session = requests.Session()
        self._is_healthy = {}  # {server_key: bool}
        self._lock = threading.Lock()
        self._check_threads = {}  # {server_key: thread}
        self.logger = logger

    def _get_key(self, server_host: str, is_fallback: bool = False) -> str:
        """Get unique key for server."""
        return f"fallback:{server_host}" if is_fallback else server_host

    def set_health_down(
        self, server_host: str, is_fallback: bool = False, health_check_fn=None
    ):
        """Mark server as down and start health checker.

        Args:
            server_host: Server URL
            is_fallback: Whether this is fallback server
            health_check_fn: Function to check health
                (signature: (session, server_host) -> bool)
        """
        key = self._get_key(server_host, is_fallback)

        with self._lock:
            if self._is_healthy.get(key, True):
                if self.logger:
                    self.logger.warning(
                        "Server marked down",
                        context={"server": key},
                        trace_id="datanadhi-health-monitor",
                        _datanadhi_internal=True,
                    )
                self._is_healthy[key] = False

            # Start health checker if not already running
            if (
                key not in self._check_threads
                or not self._check_threads[key].is_alive()
            ):
                thread = threading.Thread(
                    target=self._health_check_loop,
                    args=(server_host, is_fallback, health_check_fn),
                    daemon=True,
                    name=f"health-{key}",
                )
                thread.start()
                self._check_threads[key] = thread

    def is_server_up(self, server_host: str, is_fallback: bool = False) -> bool:
        """Check if server is marked as up.

        Args:
            server_host: Server URL
            is_fallback: Whether this is fallback server

        Returns:
            True if server is healthy, False otherwise
        """
        key = self._get_key(server_host, is_fallback)
        return self._is_healthy.get(key, True)

    def _health_check_loop(self, server_host: str, is_fallback: bool, health_check_fn):
        """Loop: check health until server is up."""
        key = self._get_key(server_host, is_fallback)

        while True:
            time.sleep(0.5)

            try:
                # Use provided health check function or default
                if health_check_fn:
                    healthy = health_check_fn(self.session, server_host)
                else:
                    healthy = self._default_health_check(server_host)

                if healthy:
                    with self._lock:
                        self._is_healthy[key] = True
                        if self.logger:
                            self.logger.debug(
                                "Server recovered",
                                context={"server": key},
                                trace_id="datanadhi-health-monitor",
                                _datanadhi_internal=True,
                            )
                    break
            except Exception as e:
                if self.logger:
                    self.logger.error(
                        "Health check error",
                        context={"server": key, "error": str(e)},
                        trace_id="datanadhi-health-monitor",
                        _datanadhi_internal=True,
                    )
                continue

    def _default_health_check(self, server_host: str) -> bool:
        """Default health check implementation."""
        try:
            response = self.session.get(f"{server_host}/", timeout=2)
            return 200 <= response.status_code < 300
        except Exception:
            return False
