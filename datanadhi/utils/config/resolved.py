import logging
from pathlib import Path

from datanadhi.utils.config.builder import ConfigBuilder
from datanadhi.utils.files import read_from_json


class ResolvedConfig:
    force_disable_echopost = False

    allowed_overrides = {
        "log_level",
        "stack_level",
        "skip_stack",
    }

    def __init__(self, datanadhi_dir: Path, **overrides):
        self.datanadhi_dir = datanadhi_dir
        self.overrides = overrides
        self.path = Path(datanadhi_dir) / ".config.resolved.json"

    def _load_or_build(self):
        if self.path.exists():
            return read_from_json(self.path)

        builder = ConfigBuilder(self.datanadhi_dir)
        return builder.build()

    def _apply_overrides(self, cfg: dict):
        for k, v in self.overrides.items():
            if k in self.allowed_overrides:
                cfg[k] = v

        cfg["stack_level"] += 2
        cfg["skip_stack"] += 4

        if ResolvedConfig.force_disable_echopost or self.overrides.get(
            "echopost_disable", False
        ):
            cfg["echopost_disable"] = True

        cfg["log_level"] = logging._nameToLevel[cfg["log_level"].upper()]
        cfg["datanadhi_log_level"] = logging._nameToLevel[
            cfg["datanadhi_log_level"].upper()
        ]
        return cfg

    def get(self):
        cfg = self._load_or_build()
        return self._apply_overrides(cfg)
