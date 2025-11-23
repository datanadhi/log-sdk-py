import contextvars
import datetime
import logging
import os
import sys
import threading
import uuid
from pathlib import Path

from dotenv import load_dotenv

from datanadhi.logger import Handler, JsonFormatter, StreamHandler
from datanadhi.utils.config import ResolvedConfig
from datanadhi.utils.echopost import ensure_binary_exists
from datanadhi.utils.rules import (
    ResolvedRules,
    RuleActions,
    RuleEvaluationResult,
    evaluate_rules,
)
from datanadhi.utils.server import trigger_pipeline

load_dotenv()

_NOT_SET = object()
trace_id_var = contextvars.ContextVar("trace_id", default=None)


class Logger:
    def __init__(
        self,
        module_name: str | None = None,
        *,
        handlers: list[Handler] | None = None,
        datanadhi_dir: Path | None = ".datanadhi",
        log_level: int | object = _NOT_SET,
        stack_level: int | object = _NOT_SET,
        skip_stack: int | object = _NOT_SET,
        echopost_disable: bool = False,
    ):
        self.module_name = module_name
        self.no_rules_set = False
        self._set_api_key()
        self.datanadhi_dir = Path(datanadhi_dir).absolute()
        self._initialise_config(
            log_level=log_level,
            stack_level=stack_level,
            skip_stack=skip_stack,
            echopost_disable=echopost_disable,
        )
        # Initialising logger before echopost so that we can log errors from echopost
        self._setup_logger(handlers)
        self._initialise_rules_and_echopost()

    @property
    def stack_level(self):
        return self.config.get("stack_level", 2)

    @property
    def skip_stack(self):
        return self.config.get("skip_stack", 4)

    def can_log(self, incoming_level: int):
        return incoming_level > self.config["datanadhi_log_level"]

    def _initialise_config(self, **config_args):
        config_args = {k: v for k, v in config_args.items() if v is not _NOT_SET}
        self.config = ResolvedConfig(self.datanadhi_dir, **config_args).get()

    def _initialise_rules_and_echopost(self):
        self.rules: RuleActions = ResolvedRules(self.datanadhi_dir).get()
        success, error = True, None
        if self.rules:
            success, error = ensure_binary_exists(self.datanadhi_dir, self.config)
        else:
            self.no_rules_set = True
            self.warning(
                "No rules set! Defaulting to stdout True",
                context={
                    "reason": "Rules Empty",
                    "datanadhi_dir": str(self.datanadhi_dir),
                },
                trace_id=f"datanadhi-internal-{self.module_name}",
                _datanadhi_internal=True,
            )
            return
        if success:
            self.debug(
                "Echopost binary now exists",
                trace_id=f"datanadhi-internal-{self.module_name}",
                _datanadhi_internal=True,
            )
        if not success:
            if "message" in error:
                message = error.get("message")
                del error["message"]
            else:
                message = "Unable to download EchoPost binary"
            self.warning(
                message,
                context=error,
                trace_id=f"datanadhi-internal-{self.module_name}",
                _datanadhi_internal=True,
            )

    def _set_api_key(self):
        env_key = os.environ.get("DATANADHI_API_KEY")
        if not env_key:
            raise ValueError(
                "API key not provided via parameter or DATANADHI_API_KEY env"
            )
        self.api_key = env_key

    def _setup_logger(self, handlers: list[Handler] | None):
        self.logger = logging.getLogger(f"{self.module_name}.{id(self)}")
        if self.logger.handlers:
            return
        if not handlers:
            handlers = [
                Handler(
                    handler=StreamHandler(sys.stdout),
                    formatter=JsonFormatter(),
                )
            ]
        for h in handlers:
            new_handler = h.handler
            new_handler.setFormatter(h.formatter or JsonFormatter())
            self.logger.addHandler(new_handler)

        self.logger.setLevel(self.config["log_level"])
        self.logger.propagate = False

    def _get_caller_info(self) -> tuple[str, int, str, str]:
        """Get information about the calling code's location.

        Uses the configured skip_stack value to determine how many frames
        to skip in the call stack. This allows accurate reporting of where
        the log call originated.

        Returns:
            tuple containing:
            - filename (str): Absolute path to the source file
            - line_number (int): Line number in the source file
            - function_name (str): Name of the calling function
            - module_name (str): Name of the module containing the call
        """
        frame = sys._getframe(self.skip_stack)
        filename = os.path.abspath(frame.f_code.co_filename)
        lineno = frame.f_lineno
        function_name = frame.f_code.co_name
        module_name = frame.f_globals.get("__name__")
        return filename, lineno, function_name, module_name

    @staticmethod
    def _get_trace_id(trace_id: str = None):
        if trace_id is not None:
            trace_id_var.set(trace_id)
        if trace_id_var.get() is None:
            trace_id_var.set(str(uuid.uuid4()))
        return trace_id_var.get()

    def _build_internal_payload(
        self, level: str, message: str, context: dict = {}
    ) -> dict:
        """Build a structured log payload with metadata.

        Creates a standardized log payload including:
        - Basic log information (message, level)
        - Temporal information (timestamp)
        - Source code location (filename, line number, etc.)
        - Trace ID for request tracking
        - Custom context data

        Args:
            level: Log level (DEBUG, INFO, etc.)
            message: The log message
            context: Additional structured data about the log event

        Returns:
            Dictionary containing the complete log payload
        """
        filename, lineno, function_name, module_name = self._get_caller_info()
        timestamp = datetime.datetime.now(datetime.UTC).isoformat() + "Z"

        return {
            "message": message,
            "trace_id": trace_id_var.get(),
            "timestamp": timestamp,
            "module_name": self.module_name,
            "log_record": {
                "filename": filename,
                "function_name": function_name,
                "level": level,
                "line_number": lineno,
                "module_name": module_name,
            },
            "context": context,
        }

    def _get_extras(self, context, trace_id, payload={}):
        timestamp = datetime.datetime.now(datetime.UTC).isoformat() + "Z"
        return {
            "timestamp": payload.get("timestamp", timestamp),
            "context": context,
            "trace_id": payload.get("trace_id", self._get_trace_id(trace_id)),
        }

    def rule_result(
        self,
        level: str,
        message: str,
        context: dict = {},
        trace_id: str | None = None,
    ) -> RuleEvaluationResult:
        """Evaluate log entry against rules and prepare the result.

        This method:
        1. Ensures a valid trace ID exists
        2. Builds the log payload
        3. Evaluates rules against the payload
        4. Returns the evaluation result

        Args:
            level: Log level (DEBUG, INFO, etc.)
            message: The log message
            context: Additional structured data
            trace_id: Optional trace ID for request tracking

        Returns:
            RuleEvaluationResult containing:
            - List of pipelines to trigger
            - Whether to output to stdout
            - The complete log payload
        """
        self._get_trace_id(trace_id)
        payload = self._build_internal_payload(level, message, context)
        pipelines, stdout_flag = evaluate_rules(payload, self.rules)
        return RuleEvaluationResult(
            pipelines=pipelines, stdout=stdout_flag, payload=payload
        )

    def trigger_pipelines(self, payload: dict, pipelines: list[str]) -> None:
        """Trigger data pipelines asynchronously.

        Each pipeline is triggered in a separate daemon thread to avoid
        blocking the logging operation.

        Args:
            payload: The log data to send to the pipeline
            pipelines: List of pipeline IDs to trigger
        """

        def trigger_pipeline_sync(pid: str, payload: dict) -> None:
            try:
                trigger_pipeline(self.api_key, pid, payload)
            except Exception as e:
                if self.log_datanadhi_errors:
                    self.logger.error(
                        f"Failed to trigger pipeline {pid}: {e}",
                        exc_info=True,
                        stacklevel=self.stacklevel,
                    )

        # Trigger each pipeline in a separate daemon thread
        for pid in pipelines:
            threading.Thread(target=trigger_pipeline_sync, args=(pid, payload)).start()

    def debug(
        self,
        message: str,
        context: dict = {},
        trace_id: str | None = None,
        _datanadhi_internal=False,
    ) -> None:
        """Log a DEBUG level message with optional context and trace ID.

        Args:
            message: The log message
            context: Additional structured data
            trace_id: Optional trace ID for request tracking
        """
        if self.no_rules_set or (_datanadhi_internal and self.can_log(logging.DEBUG)):
            extras = self._get_extras(context, trace_id)
            self.logger.debug(message, extra=extras, stacklevel=self.stack_level)
            return
        rule_result = self.rule_result("DEBUG", message, context, trace_id)
        if rule_result.stdout:
            extra = self._get_extras(context, trace_id, rule_result.payload)
            self.logger.debug(message, extra=extra, stacklevel=self.stack_level)
        self.trigger_pipelines(rule_result.payload, rule_result.pipelines)

    def info(
        self,
        message: str,
        context: dict = {},
        trace_id: str | None = None,
        _datanadhi_internal=False,
    ) -> None:
        """Log an INFO level message with optional context and trace ID.

        Args:
            message: The log message
            context: Additional structured data
            trace_id: Optional trace ID for request tracking
        """
        if self.no_rules_set or (_datanadhi_internal and self.can_log(logging.INFO)):
            extras = self._get_extras(context, trace_id)
            self.logger.info(message, extra=extras, stacklevel=self.stack_level)
            return
        rule_result = self.rule_result("INFO", message, context, trace_id)
        if rule_result.stdout:
            extra = self._get_extras(context, trace_id, rule_result.payload)
            self.logger.info(message, extra=extra, stacklevel=self.stack_level)
        self.trigger_pipelines(rule_result.payload, rule_result.pipelines)

    def warning(
        self,
        message: str,
        context: dict = {},
        trace_id: str | None = None,
        _datanadhi_internal=False,
    ) -> None:
        """Log a WARNING level message with optional context and trace ID.

        Args:
            message: The log message
            context: Additional structured data
            trace_id: Optional trace ID for request tracking
        """
        if self.no_rules_set or (_datanadhi_internal and self.can_log(logging.WARNING)):
            extras = self._get_extras(context, trace_id)
            self.logger.warning(message, extra=extras, stacklevel=self.stack_level)
            return
        rule_result = self.rule_result("WARNING", message, context, trace_id)
        if rule_result.stdout:
            extra = self._get_extras(context, trace_id, rule_result.payload)
            self.logger.warning(message, extra=extra, stacklevel=self.stack_level)
        self.trigger_pipelines(rule_result.payload, rule_result.pipelines)

    def error(
        self,
        message: str,
        context: dict = {},
        trace_id: str | None = None,
        _datanadhi_internal=False,
    ) -> None:
        """Log an ERROR level message with optional context and trace ID.

        Args:
            message: The log message
            context: Additional structured data
            trace_id: Optional trace ID for request tracking
        """
        if self.no_rules_set or (_datanadhi_internal and self.can_log(logging.ERROR)):
            extras = self._get_extras(context, trace_id)
            self.logger.error(message, extra=extras, stacklevel=self.stack_level)
            return
        rule_result = self.rule_result("ERROR", message, context, trace_id)
        if rule_result.stdout:
            extra = self._get_extras(context, trace_id, rule_result.payload)
            self.logger.error(message, extra=extra, stacklevel=self.stack_level)
        self.trigger_pipelines(rule_result.payload, rule_result.pipelines)

    def critical(
        self,
        message: str,
        context: dict = {},
        trace_id: str | None = None,
        _datanadhi_internal=False,
    ) -> None:
        """Log a CRITICAL level message with optional context and trace ID.

        Args:
            message: The log message
            context: Additional structured data
            trace_id: Optional trace ID for request tracking
        """
        if self.no_rules_set or (
            _datanadhi_internal and self.can_log(logging.CRITICAL)
        ):
            extras = self._get_extras(context, trace_id)
            self.logger.critical(message, extra=extras, stacklevel=self.stack_level)
            return
        rule_result = self.rule_result("CRITICAL", message, context, trace_id)
        if rule_result.stdout:
            extra = self._get_extras(context, trace_id, rule_result.payload)
            self.logger.critical(message, extra=extra, stacklevel=self.stack_level)
        self.trigger_pipelines(rule_result.payload, rule_result.pipelines)

    @staticmethod
    def get_record_defaults(record: logging.LogRecord) -> dict:
        """Extract default fields from a log record.

        This is a utility method for custom formatters to access standard
        log record fields in a consistent way.

        Args:
            record: The log record to process

        Returns:
            Dictionary of default log record fields
        """
        return {
            "timestamp": record.created,
            "module_name": record.name,
            "line_number": record.lineno,
            "function_name": record.funcName,
            "level": record.levelname,
            "filename": record.pathname,
        }
