import contextvars
import datetime
import os
import sys
import uuid

from datanadhi.rules import (
    RuleActions,
    RuleEvaluationResult,
    evaluate_rules,
)

STACK_LEVEL_OFFSET = 2
trace_id_var = contextvars.ContextVar("trace_id", default=None)


def _get_internal_trace_id(module_name: str) -> str:
    return f"datanadhi-internal-{module_name}"


def _get_trace_id(
    trace_id: str = None, module_name: str = None, _is_datanadhi_internal: bool = False
) -> str:
    if _is_datanadhi_internal:
        return _get_internal_trace_id(module_name)
    if trace_id is not None:
        trace_id_var.set(trace_id)
    if trace_id_var.get() is None:
        trace_id_var.set(str(uuid.uuid4()))
    return trace_id_var.get()


def _get_caller_info(skip_stack: int) -> tuple[str, int, str, str]:
    """Get information about the calling code's location.

    Uses the provided skip_stack value to determine how many frames
    to skip in the call stack. This allows accurate reporting of where
    the log call originated.

    Args:
        skip_stack: Number of stack frames to skip

    Returns:
        tuple containing:
        - filename (str): Absolute path to the source file
        - line_number (int): Line number in the source file
        - function_name (str): Name of the calling function
        - module_name (str): Name of the module containing the call
    """
    frame = sys._getframe(skip_stack)
    filename = os.path.abspath(frame.f_code.co_filename)
    lineno = frame.f_lineno
    function_name = frame.f_code.co_name
    module_name = frame.f_globals.get("__name__")
    return filename, lineno, function_name, module_name


def _build_internal_payload(
    level: str,
    stacklevel: int,
    message: str,
    given_module_name: str = "",
    context: dict = {},
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
        stacklevel: Optional stacklevel override for caller info

    Returns:
        Dictionary containing the complete log payload
    """
    skip_stack = stacklevel + STACK_LEVEL_OFFSET
    filename, lineno, function_name, module_name = _get_caller_info(skip_stack)
    timestamp = datetime.datetime.now(datetime.UTC).isoformat() + "Z"

    return {
        "message": message,
        "trace_id": trace_id_var.get(),
        "timestamp": timestamp,
        "module_name": given_module_name,
        "log_record": {
            "filename": filename,
            "function_name": function_name,
            "level": level,
            "line_number": lineno,
            "module_name": module_name,
        },
        "context": context,
    }


def get_rule_result(
    rules: RuleActions,
    module_name: str,
    level: str,
    stacklevel: int,
    message: str,
    context: dict = {},
    trace_id: str | None = None,
    _is_datanadhi_internal: bool = False,
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
        stacklevel: Optional stacklevel override

    Returns:
        RuleEvaluationResult containing:
        - List of pipelines to trigger
        - Whether to output to stdout
        - The complete log payload
    """
    _get_trace_id(trace_id, module_name, _is_datanadhi_internal)
    payload = _build_internal_payload(level, stacklevel, message, module_name, context)
    pipelines, stdout_flag = evaluate_rules(payload, rules)
    return RuleEvaluationResult(
        pipelines=pipelines, stdout=stdout_flag, payload=payload
    )


def get_extras(
    context, trace_id, module_name=None, _is_datanadhi_internal=False, payload={}
):
    timestamp = datetime.datetime.now(datetime.UTC).isoformat() + "Z"
    return {
        "timestamp": payload.get("timestamp", timestamp),
        "context": context,
        "trace_id": payload.get(
            "trace_id", _get_trace_id(trace_id, module_name, _is_datanadhi_internal)
        ),
    }
