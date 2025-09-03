import logging
from pythonjsonlogger import jsonlogger

def init_logger(name: str) -> logging.Logger:
    """
    Initializes and returns a logger with JSON-formatted output.

    This function sets up a logger with the specified name, configured to log
    messages to standard output in JSON format. It uses the `python-json-logger`
    to produce structured logs, which include timestamp, log level, logger name,
    and message. If a logger with the given name already exists and has handlers,
    no additional handlers are added.

    Args:
        name (str): Name of the logger to create or retrieve.

    Returns:
        logging.Logger: Configured logger instance with JSON formatting.

    Example:
        >>> logger = init_logger("my_service")
        >>> logger.info("Application started")
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setFormatter(jsonlogger.JsonFormatter("%(asctime)s %(levelname)s %(name)s %(message)s"))

    if not logger.handlers:
        logger.addHandler(handler)

    logger.propagate = False
    
    return logger
