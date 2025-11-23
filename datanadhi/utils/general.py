from pathlib import Path

import yaml


def load_from_yaml(path: Path):
    with open(path) as f:
        return yaml.safe_load(f)
