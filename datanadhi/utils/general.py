from pathlib import Path

import yaml


def load_from_yaml(path: Path):
    """Load YAML file and return parsed content."""
    with open(path) as f:
        return yaml.safe_load(f)
