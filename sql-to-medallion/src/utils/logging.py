import logging
import sys

def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Configures and returns a logger instance.

    Args:
        name: The name for the logger.
        level: The logging level (e.g., logging.INFO, logging.DEBUG).

    Returns:
        A configured logger instance.
    """
    logger = logging.getLogger(name)
    
    # Prevent duplicate handlers if logger already exists
    if not logger.handlers:
        logger.setLevel(level)
        
        # Create handler
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)
        
        # Create formatter and add it to the handler
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        
        # Add the handler to the logger
        logger.addHandler(handler)
        
    logger.propagate = False # Prevent propagation to root logger if configured elsewhere (like in Databricks)
    
    return logger

# Example usage:
# logger = get_logger(__name__)
# logger.info("This is an info message.")
# logger.error("This is an error message.") 