import os
from pathlib import Path

from datanadhi.utils.files import load_from_yaml, write_to_json


class ConfigBuilder:
    def __init__(self, datanadhi_dir: Path):
        self.datanadhi_dir = datanadhi_dir
        self.config_yaml = {}

        self.mapping = {
            "server_host": {
                "config": "server.host",
                "env": "DATANADHI_SERVER_HOST",
                "default": "http://data-nadhi-server:5000",
            },
            "fallback_server_host": {
                "config": "server.fallback_host",
                "env": "DATANADHI_FALLBACK_SERVER_HOST",
                "default": "http://datanadhi-fallback-server:5001",
            },
            "log_level": {
                "config": "log.level",
                "default": "INFO",
            },
            "stack_level": {
                "config": "log.stack_level",
                "default": 0,
            },
            "skip_stack": {
                "config": "log.skip_stack",
                "default": 0,
            },
            "datanadhi_log_level": {
                "config": "log.datanadhi_log_level",
                "default": "INFO",
            },
            "echopost_disable": {
                "config": "echopost.disable",
                "default": False,
            },
            "async_queue_size": {
                "config": "async.queue_size",
                "env": "DATANADHI_QUEUE_SIZE",
                "default": 1000,
            },
            "async_workers": {
                "config": "async.workers",
                "env": "DATANADHI_WORKERS",
                "default": 2,
            },
            "async_exit_timeout": {
                "config": "async.exit_timeout",
                "env": "DATANADHI_EXIT_TIMEOUT",
                "default": 5,
            },
        }

        self._load_yaml()

    def _load_yaml(self):
        for name in ("config.yml", "config.yaml"):
            p = Path(self.datanadhi_dir) / name
            if p.exists():
                self.config_yaml = load_from_yaml(p)
                return
        self.config_yaml = {}

    def _from_yaml(self, path: str):
        cur = self.config_yaml
        for part in path.split("."):
            if not isinstance(cur, dict) or part not in cur:
                return None
            cur = cur[part]
        return cur

    def _resolve(self, spec: dict):
        if spec.get("config"):
            val = self._from_yaml(spec["config"])
            if val is not None:
                return val

        if spec.get("env"):
            val = os.getenv(spec["env"])
            if val is not None:
                return val

        return spec.get("default")

    def build(self):
        resolved = {k: self._resolve(spec) for k, spec in self.mapping.items()}
        if resolved["server_host"].endswith("/"):
            resolved["server_host"] = resolved["server_host"][:-1]
        if resolved["fallback_server_host"].endswith("/"):
            resolved["fallback_server_host"] = resolved["fallback_server_host"][:-1]
        out_path = Path(self.datanadhi_dir) / ".config.resolved.json"
        write_to_json(out_path, resolved)

        return resolved
