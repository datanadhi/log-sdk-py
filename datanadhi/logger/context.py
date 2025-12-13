import sys
import traceback

STACK_LEVEL_OFFSET = 2


def _extract_exception_info() -> dict | None:
    """Extract structured exception info from current exception context.

    Returns:
        Dict with exception details or None if no exception active
    """
    exc_info = sys.exc_info()
    if exc_info[0] is None:
        return None

    exc_type, exc_value, exc_tb = exc_info

    # Get the traceback frames
    tb_frames = traceback.extract_tb(exc_tb)
    last_frame = tb_frames[-1] if tb_frames else None

    return {
        "type": exc_type.__name__ if exc_type else None,
        "message": str(exc_value) if exc_value else None,
        "stacktrace": " ".join(traceback.format_exception(*exc_info)),
        "file": last_frame.filename if last_frame else None,
        "line": last_frame.lineno if last_frame else None,
        "function": last_frame.name if last_frame else None,
    }


def _extract_stack_info(stack_level) -> dict | None:
    """Extract current stack trace.

    Returns:
        Dict with stack trace or None
    """
    stack = traceback.format_stack()[:-stack_level]  # Exclude this function
    return {"stacktrace": "".join(stack)}


def get_context_stack_level(config, context, stack_info, exc_info, stack_level, kwargs):
    if kwargs:
        context = {**context, **kwargs}

    stack_level = (
        stack_level + STACK_LEVEL_OFFSET
        if stack_level is not None
        else config.get("stack_level", STACK_LEVEL_OFFSET)
    )

    # Add exception info to context if requested
    if exc_info:
        error_info = _extract_exception_info()
        if error_info:
            context = {**context, "error": error_info}

    # Add stack info to context if requested
    if stack_info:
        stack_data = _extract_stack_info(stack_level)
        if stack_data:
            context = {**context, "stack": stack_data}

    return context, stack_level
