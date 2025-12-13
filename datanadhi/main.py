import contextvars
import logging
import os
from pathlib import Path

from dotenv import load_dotenv

from datanadhi.async_processing import get_processor_for_directory
from datanadhi.config import ResolvedConfig
from datanadhi.echopost import ensure_binary_exists
from datanadhi.logger import Handler, get_logger
from datanadhi.logger.context import get_context_stack_level
from datanadhi.rules import (
    ResolvedRules,
    RuleActions,
)
from datanadhi.rules.core import get_extras, get_rule_result

load_dotenv()

_NOT_SET = object()
trace_id_var = contextvars.ContextVar("trace_id", default=None)

# Global constant for stack level offset
STACK_LEVEL_OFFSET = 2


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
        self.config = ResolvedConfig(
            self.datanadhi_dir,
            _NOT_SET,
            log_level=log_level,
            stack_level=stack_level,
            skip_stack=skip_stack,
            echopost_disable=echopost_disable,
        ).get()
        # Initialising logger before echopost so that we can log errors from echopost
        self.logger = get_logger(
            handlers, self.config["log_level"], self.module_name, id(self)
        )
        self._initialise_rules_and_echopost()
        # Initialize async processor for non-blocking pipeline triggers
        self._processor = None
        self._initialise_processor()

    def can_log(self, incoming_level: int):
        return incoming_level > self.config["datanadhi_log_level"]

    @property
    def _get_internal_trace_id(self) -> str:
        return f"datanadhi-internal-{self.module_name}"

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

    def _initialise_processor(self):
        """Initialize async processor."""
        if not self.no_rules_set and self.rules:
            config_with_key = {
                **self.config,
                "api_key": self.api_key,
            }
            self._processor = get_processor_for_directory(
                self.datanadhi_dir, config_with_key, self
            )

    def _set_api_key(self):
        env_key = os.environ.get("DATANADHI_API_KEY")
        if not env_key:
            raise ValueError(
                "API key not provided via parameter or DATANADHI_API_KEY env"
            )
        self.api_key = env_key

    def trigger_pipelines(self, payload: dict, pipelines: list[str]) -> None:
        """Trigger data pipelines asynchronously via queue-based processor.

        Pipelines are enqueued for processing by background worker threads.
        This method returns immediately without blocking the logging operation.

        Args:
            payload: The log data to send to the pipeline
            pipelines: List of pipeline IDs to trigger
        """
        if not hasattr(self, "_processor") or not self._processor or not pipelines:
            return  # Processor not initialized (no rules or disabled)

        self._processor.submit(pipelines, payload)

    def debug(
        self,
        message: str,
        context: dict = {},
        trace_id: str | None = None,
        exc_info: bool = False,
        stack_info: bool = False,
        stacklevel: int | None = None,
        _datanadhi_internal=False,
        **kwargs,
    ) -> dict | None:
        """Log a DEBUG level message with optional context and trace ID.

        Args:
            message: The log message
            context: Additional structured data
            trace_id: Optional trace ID for request tracking
            exc_info: Include exception information
            stack_info: Include stack trace
            stacklevel: Override stack level for caller detection
            **kwargs: Additional context fields

        Returns:
            Internal payload dict if rules are set, None otherwise
        """
        context, stack_level = get_context_stack_level(
            self.config, context, stack_info, exc_info, stacklevel, kwargs
        )

        if self.no_rules_set or (_datanadhi_internal and self.can_log(logging.DEBUG)):
            extras = get_extras(
                context, trace_id, self.module_name, _datanadhi_internal
            )
            self.logger.debug(
                message,
                extra=extras,
                stacklevel=stack_level,
                exc_info=exc_info,
                stack_info=stack_info,
            )
            return None
        if _datanadhi_internal:
            return None

        rule_result = get_rule_result(
            self.rules,
            self.module_name,
            "DEBUG",
            stack_level,
            message,
            context,
            trace_id,
            _datanadhi_internal,
        )
        if rule_result.stdout:
            extra = get_extras(
                context,
                trace_id,
                self.module_name,
                _datanadhi_internal,
                rule_result.payload,
            )
            self.logger.debug(
                message,
                extra=extra,
                stacklevel=stack_level,
                exc_info=exc_info,
                stack_info=stack_info,
            )
        self.trigger_pipelines(rule_result.payload, rule_result.pipelines)
        return rule_result.payload

    def info(
        self,
        message: str,
        context: dict = {},
        trace_id: str | None = None,
        exc_info: bool = False,
        stack_info: bool = False,
        stacklevel: int | None = None,
        _datanadhi_internal=False,
        **kwargs,
    ) -> dict | None:
        """Log an INFO level message with optional context and trace ID.

        Args:
            message: The log message
            context: Additional structured data
            trace_id: Optional trace ID for request tracking
            exc_info: Include exception information
            stack_info: Include stack trace
            stacklevel: Override stack level for caller detection
            **kwargs: Additional context fields

        Returns:
            Internal payload dict if rules are set, None otherwise
        """
        context, stack_level = get_context_stack_level(
            self.config, context, stack_info, exc_info, stacklevel, kwargs
        )

        if self.no_rules_set or (_datanadhi_internal and self.can_log(logging.INFO)):
            extras = get_extras(
                context, trace_id, self.module_name, _datanadhi_internal
            )
            self.logger.info(
                message,
                extra=extras,
                stacklevel=stack_level,
                exc_info=exc_info,
                stack_info=stack_info,
            )
            return None

        rule_result = get_rule_result(
            self.rules,
            self.module_name,
            "INFO",
            stack_level,
            message,
            context,
            trace_id,
            _datanadhi_internal,
        )
        if rule_result.stdout:
            extra = get_extras(
                context,
                trace_id,
                self.module_name,
                _datanadhi_internal,
                rule_result.payload,
            )
            self.logger.info(
                message,
                extra=extra,
                stacklevel=stack_level,
                exc_info=exc_info,
                stack_info=stack_info,
            )
        self.trigger_pipelines(rule_result.payload, rule_result.pipelines)
        return rule_result.payload

    def warning(
        self,
        message: str,
        context: dict = {},
        trace_id: str | None = None,
        exc_info: bool = False,
        stack_info: bool = False,
        stacklevel: int | None = None,
        _datanadhi_internal=False,
        **kwargs,
    ) -> dict | None:
        """Log a WARNING level message with optional context and trace ID.

        Args:
            message: The log message
            context: Additional structured data
            trace_id: Optional trace ID for request tracking
            exc_info: Include exception information
            stack_info: Include stack trace
            stacklevel: Override stack level for caller detection
            **kwargs: Additional context fields

        Returns:
            Internal payload dict if rules are set, None otherwise
        """
        context, stack_level = get_context_stack_level(
            self.config, context, stack_info, exc_info, stacklevel, kwargs
        )

        if self.no_rules_set or (_datanadhi_internal and self.can_log(logging.WARNING)):
            extras = get_extras(
                context, trace_id, self.module_name, _datanadhi_internal
            )
            self.logger.warning(
                message,
                extra=extras,
                stacklevel=stack_level,
                exc_info=exc_info,
                stack_info=stack_info,
            )
            return None

        rule_result = get_rule_result(
            self.rules,
            self.module_name,
            "WARNING",
            stack_level,
            message,
            context,
            trace_id,
            _datanadhi_internal,
        )
        if rule_result.stdout:
            extra = get_extras(
                context,
                trace_id,
                self.module_name,
                _datanadhi_internal,
                rule_result.payload,
            )
            self.logger.warning(
                message,
                extra=extra,
                stacklevel=stack_level,
                exc_info=exc_info,
                stack_info=stack_info,
            )
        self.trigger_pipelines(rule_result.payload, rule_result.pipelines)
        return rule_result.payload

    def error(
        self,
        message: str,
        context: dict = {},
        trace_id: str | None = None,
        exc_info: bool = False,
        stack_info: bool = False,
        stacklevel: int | None = None,
        _datanadhi_internal=False,
        **kwargs,
    ) -> dict | None:
        """Log an ERROR level message with optional context and trace ID.

        Args:
            message: The log message
            context: Additional structured data
            trace_id: Optional trace ID for request tracking
            exc_info: Include exception information
            stack_info: Include stack trace
            stacklevel: Override stack level for caller detection
            **kwargs: Additional context fields

        Returns:
            Internal payload dict if rules are set, None otherwise
        """
        context, stack_level = get_context_stack_level(
            self.config, context, stack_info, exc_info, stacklevel, kwargs
        )

        if self.no_rules_set or (_datanadhi_internal and self.can_log(logging.ERROR)):
            extras = get_extras(
                context, trace_id, self.module_name, _datanadhi_internal
            )
            self.logger.error(
                message,
                extra=extras,
                stacklevel=stack_level,
                exc_info=exc_info,
                stack_info=stack_info,
            )
            return None

        rule_result = get_rule_result(
            self.rules,
            self.module_name,
            "ERROR",
            stack_level,
            message,
            context,
            trace_id,
            _datanadhi_internal,
        )
        if rule_result.stdout:
            extra = get_extras(
                context,
                trace_id,
                self.module_name,
                _datanadhi_internal,
                rule_result.payload,
            )
            self.logger.error(
                message,
                extra=extra,
                stacklevel=stack_level,
                exc_info=exc_info,
                stack_info=stack_info,
            )
        self.trigger_pipelines(rule_result.payload, rule_result.pipelines)
        return rule_result.payload

    def critical(
        self,
        message: str,
        context: dict = {},
        trace_id: str | None = None,
        exc_info: bool = False,
        stack_info: bool = False,
        stacklevel: int | None = None,
        _datanadhi_internal=False,
        **kwargs,
    ) -> dict | None:
        """Log a CRITICAL level message with optional context and trace ID.

        Args:
            message: The log message
            context: Additional structured data
            trace_id: Optional trace ID for request tracking
            exc_info: Include exception information
            stack_info: Include stack trace
            stacklevel: Override stack level for caller detection
            **kwargs: Additional context fields

        Returns:
            Internal payload dict if rules are set, None otherwise
        """
        context, stack_level = get_context_stack_level(
            self.config, context, stack_info, exc_info, stacklevel, kwargs
        )

        if self.no_rules_set or (
            _datanadhi_internal and self.can_log(logging.CRITICAL)
        ):
            extras = get_extras(
                context, trace_id, self.module_name, _datanadhi_internal
            )
            self.logger.critical(
                message,
                extra=extras,
                stacklevel=stack_level,
                exc_info=exc_info,
                stack_info=stack_info,
            )
            return None

        rule_result = get_rule_result(
            self.rules,
            self.module_name,
            "CRITICAL",
            stack_level,
            message,
            context,
            trace_id,
            _datanadhi_internal,
        )
        if rule_result.stdout:
            extra = get_extras(
                context,
                trace_id,
                self.module_name,
                _datanadhi_internal,
                rule_result.payload,
            )
            self.logger.critical(
                message,
                extra=extra,
                stacklevel=stack_level,
                exc_info=exc_info,
                stack_info=stack_info,
            )
        self.trigger_pipelines(rule_result.payload, rule_result.pipelines)
        return rule_result.payload

    def exception(
        self,
        message: str,
        context: dict = {},
        trace_id: str | None = None,
        exc_info: bool = True,
        stack_info: bool = False,
        stacklevel: int | None = None,
        _datanadhi_internal=False,
        **kwargs,
    ) -> dict | None:
        """Log an ERROR level message with exception info.

        Automatically captures exception traceback when called in except block.
        This is equivalent to error() with exc_info=True.

        Args:
            message: The log message
            context: Additional structured data
            trace_id: Optional trace ID for request tracking
            exc_info: Include exception information (default True)
            stack_info: Include stack trace
            stacklevel: Override stack level for caller detection
            **kwargs: Additional context fields

        Returns:
            Internal payload dict if rules are set, None otherwise
        """
        stack_level = (
            stacklevel if stacklevel is not None else self.config.get("stack_level", 2)
        )
        return self.error(
            message=message,
            context=context,
            trace_id=trace_id,
            exc_info=exc_info,
            stack_info=stack_info,
            stacklevel=stack_level + 1,
            _datanadhi_internal=_datanadhi_internal,
            **kwargs,
        )

    def wait_till_logs_pushed(self):
        """Wait until all queued pipeline triggers are processed."""
        if self._processor:
            self._processor._wait_till_drain_complete()

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
