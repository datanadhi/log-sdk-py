import platform
import urllib.error
import urllib.request
from pathlib import Path

from datanadhi.utils.config import ResolvedConfig


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


def get_core_dir(datanadhi_dir: Path):
    return Path(datanadhi_dir) / "core"


def get_binary_path(datanadhi_dir: Path):
    return get_core_dir(datanadhi_dir) / "echopost"


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
        get_core_dir(datanadhi_dir).mkdir(parents=True, exist_ok=True)
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
