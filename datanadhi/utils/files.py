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
