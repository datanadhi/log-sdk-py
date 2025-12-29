import logging
import sys

from datanadhi.logger.handler import FileHandler, Formatter, Handler, StreamHandler
from datanadhi.logger.json_formatter import JsonFormatter

__all__ = ["FileHandler", "StreamHandler", "Formatter", "JsonFormatter", "Handler"]


def get_logger(handlers, log_level, module_name, object_id):
    """Get or create a logger with the specified configuration.
    
    Returns existing logger if already configured, otherwise creates new one
    with provided handlers and log level.
    """
    logger = logging.getLogger(f"{module_name}.{object_id}")
    if logger.handlers:
        return logger
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
        logger.addHandler(new_handler)

    logger.setLevel(log_level)
    logger.propagate = False

    return logger
