"""
Logging configuration for the SQL Migration Agents.
"""

import os
import sys
from loguru import logger

def setup_logger():
    """
    Configure the logger for the application.
    Sets up console and file logging with proper formatting.
    """
    log_level = os.getenv("LOG_LEVEL", "INFO")
    log_file = os.getenv("LOG_FILE", "logs/sql_migration.log")
    
    # Create logs directory if it doesn't exist
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    # Configure logger - remove default handlers and add custom ones
    logger.remove()
    
    # Add console handler
    logger.add(sys.stdout, level=log_level)
    
    # Add file handler
    logger.add(
        log_file,
        rotation="10 MB",
        retention="1 week",
        level=log_level
    )
    
    logger.info(f"Logger initialized with level: {log_level}")
    return logger 