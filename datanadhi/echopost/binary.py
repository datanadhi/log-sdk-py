import platform
import subprocess
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

import grpc
import orjson

import datanadhi.echopost.grpc.logagent_pb2 as logagent_pb2
import datanadhi.echopost.grpc.logagent_pb2_grpc as logagent_pb2_grpc
from datanadhi.config import ResolvedConfig

_LOCKS = {}
_LOCKS_GUARD = threading.Lock()


def get_download_url():
    system = platform.system().lower()
    machine = platform.machine().lower()

    if system in ["darwin", "linux"]:
        os_name = system
    else:
        return False, {
            "message": "Unsupported OS for EchoPost",
            "system": system,
            "supported_systems": ["darwin", "linux"],
        }

    if machine in ("x86_64", "amd64"):
        arch = "amd64"
    elif machine in ("arm64", "aarch64"):
        arch = "arm64"
    else:
        return False, {
            "message": "Unsupported machine type for EchoPost",
            "machine_type": machine,
            "supported_machines": ["x86_64", "amd64", "arm64", "aarch64"],
        }

    return (
        True,
        f"https://downloads.datanadhi.com/echopost/{os_name}/{arch}/echopost-latest",
    )


def get_start_lock(datanadhi_dir: Path) -> threading.Lock:
    key = str(datanadhi_dir)
    with _LOCKS_GUARD:
        if key not in _LOCKS:
            _LOCKS[key] = threading.Lock()
        return _LOCKS[key]


def get_echopost_dir(datanadhi_dir: Path):
    return Path(datanadhi_dir) / "echopost"


def get_binary_path(datanadhi_dir: Path):
    return get_echopost_dir(datanadhi_dir) / "echopost"


def get_socket_path(datanadhi_dir: Path):
    return get_echopost_dir(datanadhi_dir) / "data-nadhi-agent.sock"


def ensure_binary_exists(datanadhi_dir: Path, resolved_config: dict):
    """
    Ensure echopost binary exists.
    Return: (available: bool, info: dict | None)
    """
    if resolved_config.get("echopost_disable"):
        return False, {"disabled": True}

    binary_path = get_binary_path(datanadhi_dir)

    if binary_path.exists():
        return True, None

    try:
        get_echopost_dir(datanadhi_dir).mkdir(parents=True, exist_ok=True)
        success, url = get_download_url()
        if not success:
            return success, url

        try:
            with urllib.request.urlopen(url) as resp:
                status = resp.getcode()
                status_success = 200
                if status != status_success:
                    return False, {
                        "type": "http_error",
                        "status": status,
                        "detail": f"Download returned status {status}",
                    }
                data = resp.read()

        except urllib.error.HTTPError as e:
            return False, {"type": "http_error", "status": e.code, "detail": e.reason}

        except urllib.error.URLError as e:
            ResolvedConfig.force_disable_echopost = True
            return False, {"type": "network_error", "detail": str(e)}

        with open(binary_path, "wb") as f:
            f.write(data)
        binary_path.chmod(0o755)
        return True, None

    except Exception as e:
        ResolvedConfig.force_disable_echopost = True
        return False, {"type": "unknown_error", "detail": str(e)}


def start_echopost_detached(
    datanadhi_dir: Path, api_key: str, server_host: str
) -> bool:
    """
    Start the echopost binary as an independent, detached background process.
    Returns True if the spawn succeeded (does not guarantee socket readiness).
    """
    binary_path = get_binary_path(datanadhi_dir)
    echopost_dir = get_echopost_dir(datanadhi_dir)

    if not binary_path.exists():
        return False

    try:
        delete_socket_if_exists(datanadhi_dir)

        subprocess.Popen(
            [
                str(binary_path.absolute()),
                "-datanadhi",
                str(echopost_dir.absolute()),
                "-api-key",
                api_key,
                "-health-url",
                server_host,
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
            close_fds=True,
            start_new_session=True,
        )
        return True
    except Exception:
        return False


def send_log_over_unix_grpc(
    datanadhi_dir: Path, pipelines: list[str], payload: dict, api_key: str
) -> bool:
    """Perform the SendLog RPC over a unix domain socket and return a
    dict with 'success' and 'message'."""
    socket_path = get_socket_path(datanadhi_dir)
    target = f"unix://{str(socket_path)}"
    req = logagent_pb2.LogRequest(
        json_data=orjson.dumps(payload).decode(),
        pipelines=pipelines,
        api_key=api_key,
    )

    try:
        # use insecure_channel; close when leaving context
        with grpc.insecure_channel(target) as channel:
            stub = logagent_pb2_grpc.LogAgentStub(channel)
            resp = stub.SendLog(req)
            return resp.success
    except Exception:
        return False


def socket_exists(datanadhi_dir: Path) -> bool:
    """Check if the socket file exists."""
    socket_path = get_socket_path(datanadhi_dir)
    return socket_path.exists()


def delete_socket_if_exists(datanadhi_dir: Path):
    """Delete the socket file if it exists."""
    socket_path = get_socket_path(datanadhi_dir)
    try:
        if socket_path.exists():
            socket_path.unlink()
    except Exception:
        # Socket deletion failure is non-critical, ignore
        pass


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


def start_if_socket_not_exists(
    datanadhi_dir: Path, api_key: str, server_host: str
) -> bool:
    if not socket_exists(datanadhi_dir):
        lock = get_start_lock(datanadhi_dir)
        with lock:
            if not socket_exists(datanadhi_dir):
                started = start_echopost_detached(datanadhi_dir, api_key, server_host)
                if not started:
                    return False
        return wait_for_socket(datanadhi_dir)
    return True
