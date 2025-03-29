"""
LLM Configuration for the SQL Migration Agents.
Handles setup for both Azure OpenAI and OpenAI.
"""

import os
from typing import Dict, List, Any
from loguru import logger

# Default values
DEFAULT_TEMPERATURE = 0.2
DEFAULT_TIMEOUT = 120
DEFAULT_MAX_TOKENS = 4096 # Example default, adjust as needed

def get_config_list() -> List[Dict[str, Any]]:
    """
    Create configuration list for AutoGen based on environment variables.
    Supports both Azure OpenAI and OpenAI.
    
    Returns:
        List of configuration dictionaries for AutoGen
    """
    # Check if using Azure OpenAI
    if os.getenv("AZURE_OPENAI_API_KEY") and os.getenv("AZURE_OPENAI_ENDPOINT"):
        logger.info("Using Azure OpenAI API")
        config_list = [
            {
                "model": os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "gpt-4"),
                "api_key": os.getenv("AZURE_OPENAI_API_KEY"),
                "api_base": os.getenv("AZURE_OPENAI_ENDPOINT"),
                "api_type": "azure",
                "api_version": os.getenv("AZURE_OPENAI_API_VERSION", "2023-12-01-preview"),
            }
        ]
    # Fallback to OpenAI
    elif os.getenv("OPENAI_API_KEY"):
        logger.info("Using OpenAI API")
        config_list = [
            {
                "model": os.getenv("OPENAI_MODEL", "gpt-4"),
                "api_key": os.getenv("OPENAI_API_KEY"),
            }
        ]
    else:
        logger.error("No API keys found for OpenAI or Azure OpenAI. Please check your .env file.")
        config_list = []
    
    return config_list

def get_llm_config() -> Dict[str, Any]:
    """
    Create LLM configuration for AutoGen based on environment variables.
    
    Returns:
        LLM configuration dictionary for AutoGen
    """
    config_list = get_config_list()
    if not config_list:
      # Return an empty config if no API keys found to avoid downstream errors
      return {}

    # Read configuration with defaults
    temperature = float(os.getenv("TEMPERATURE", DEFAULT_TEMPERATURE))
    timeout = int(os.getenv("TIMEOUT_SECONDS", DEFAULT_TIMEOUT))
    max_tokens = int(os.getenv("MAX_TOKENS", DEFAULT_MAX_TOKENS))

    logger.debug(f"LLM Config: Temperature={temperature}, Timeout={timeout}, Max Tokens={max_tokens}")

    return {
        "config_list": config_list,
        "temperature": temperature,
        "timeout": timeout,
        "max_tokens": max_tokens,
        # Note: Other parameters like frequency_penalty, presence_penalty etc.
        # could also be added here if needed and supported by the model/AutoGen.
    } 