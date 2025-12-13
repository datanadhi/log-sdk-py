import time
from pathlib import Path

import orjson
import yaml


def load_from_yaml(path: Path):
    with open(path) as f:
        return yaml.safe_load(f)


def write_to_json(path: Path, data: dict):
    with open(path, "wb") as f:
        blob = orjson.dumps(data)
        f.write(blob)


def read_from_json(path: Path):
    with open(path, "rb") as f:
        return orjson.loads(f.read())


def store_dropped_data(datanadhi_dir: Path, items: list, reason: str) -> str:
    """Store dropped data to file and return file path.

    Args:
        datanadhi_dir: Base datanadhi directory
        items: List of dropped items (tuples of (pipelines, payload))
        reason: Reason for dropping (e.g., 'primary_failed', 'fallback_failed')

    Returns:
        Relative path to the stored file
    """
    dropped_dir = Path(datanadhi_dir) / "dropped"
    dropped_dir.mkdir(parents=True, exist_ok=True)

    timestamp = int(time.time() * 1000)  # milliseconds
    filename = f"{reason}_{timestamp}.jsonl"
    file_path = dropped_dir / filename

    # Write as JSONL
    with open(file_path, "wb") as f:
        for item in items:
            pipelines, payload = item
            record = {"pipelines": pipelines, "log_data": payload}
            f.write(orjson.dumps(record) + b"\n")

    # Return relative path
    return str(file_path.relative_to(datanadhi_dir))
