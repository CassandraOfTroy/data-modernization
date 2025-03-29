"""
File utilities for SQL Migration Agents.
"""

import os
import json
from typing import Dict, Any, Union
from loguru import logger

def read_sql_file(sql_file: str) -> str:
    """
    Read SQL code from a file with error handling.
    
    Args:
        sql_file: Path to the SQL file to read
        
    Returns:
        The SQL code as a string
        
    Raises:
        FileNotFoundError: If the file doesn't exist
        IOError: If there's an error reading the file
    """
    try:
        with open(sql_file, "r") as f:
            sql_code = f.read()
        return sql_code
    except FileNotFoundError:
        logger.error(f"SQL file not found: {sql_file}")
        raise FileNotFoundError(f"SQL file not found: {sql_file}")
    except IOError as e:
        logger.error(f"Error reading SQL file {sql_file}: {e}")
        raise IOError(f"Error reading SQL file: {e}")

def write_output_file(output_path: str, content: Union[str, Dict[str, Any]], is_json: bool = False) -> bool:
    """
    Write content to an output file with error handling.
    
    Args:
        output_path: Path to the output file
        content: Content to write (string or dict for JSON)
        is_json: Whether to write as JSON
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
        
        with open(output_path, "w") as f:
            if is_json:
                json.dump(content, f, indent=2)
            else:
                f.write(content)
        logger.debug(f"Successfully wrote to file: {output_path}")
        return True
    except IOError as e:
        logger.error(f"Error writing to {output_path}: {e}")
        return False 