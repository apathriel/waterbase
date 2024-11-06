from pathlib import Path
import logging
from typing import Optional


class LoggingConfig:
    def init(self, log_level: str = "INFO", log_file: Optional[Path] = None):
        self.log_level = getattr(logging, log_level.upper(), logging.INFO)
        self.log_file = log_file

    def setup_logging(self):
        pass


def get_logger(
    name: str, level: str = "INFO", log_file: Optional[str] = None
) -> logging.Logger:
    """
    Creates and configures a logger with the specified name and logging level.
    Optionally, logs can be exported to a specified file.

    Parameters:
        name (str): The name of the logger. Convention is to use __name__.
        level (str): The logging level as a string. Default is 'INFO'.
        log_file (Optional[str]): The file path to export logs. Default is None.

    Returns:
        logging.Logger: The configured logger object.
    """
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    logger = logging.getLogger(name)
    logger.setLevel(numeric_level)

    # Stream handler for console output
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(
        logging.Formatter("[%(levelname)s] - %(asctime)s - %(funcName)s - %(message)s")
    )
    logger.addHandler(stream_handler)

    # File handler for file output
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(
            logging.Formatter(
                "[%(levelname)s] - %(asctime)s - %(funcName)s - %(message)s"
            )
        )
        logger.addHandler(file_handler)

    return logger
