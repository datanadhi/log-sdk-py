import threading
import time
from pathlib import Path

from datanadhi.echopost.binary import (
    send_log_over_unix_grpc,
    socket_exists,
    start_echopost_detached,
)


def wait_for_socket(
    datanadhi_dir: Path, timeout: float = 2.0, poll_interval: float = 0.05
) -> bool:
    """Wait up to `timeout` seconds for socket_path to exist."""
    start = time.time()
    while time.time() - start < timeout:
        if socket_exists(datanadhi_dir):
            return True
        time.sleep(poll_interval)
    return False


class EchoPostLink:
    def __init__(self):
        self._LOCKS = {}
        self._LOCKS_GUARD = threading.Lock()

    def get_start_lock(self, datanadhi_dir: Path) -> threading.Lock:
        key = str(datanadhi_dir)
        with self._LOCKS_GUARD:
            if key not in self._LOCKS:
                self._LOCKS[key] = threading.Lock()
            return self._LOCKS[key]

    def send_to_agent(
        self,
        datanadhi_dir: Path,
        item: tuple[list[str], dict],
        api_key: str,
        server_host: str,
    ):
        if not socket_exists(datanadhi_dir):
            lock = self.get_start_lock(datanadhi_dir)
            with lock:
                if not socket_exists(datanadhi_dir):
                    started = start_echopost_detached(
                        datanadhi_dir, api_key, server_host
                    )
                    if not started:
                        return False
            ok = wait_for_socket(datanadhi_dir)
            if not ok:
                return False
        if socket_exists(datanadhi_dir):
            try:
                return send_log_over_unix_grpc(
                    datanadhi_dir,
                    pipelines=item[0],
                    payload=item[1],
                    api_key=api_key,
                )

            except Exception:
                return False
        else:
            return False
