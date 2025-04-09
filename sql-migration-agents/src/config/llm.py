"""
LLM Configuration for the SQL Migration Agents.
Handles setup for both Azure OpenAI and OpenAI.
"""

import os
from typing import Dict, List, Any
from loguru import logger

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
        
        api_version = os.getenv("AZURE_OPENAI_API_VERSION")
        if not api_version:
            logger.error("Missing AZURE_OPENAI_API_VERSION in environment")
            return []
            
        deployment_name = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME")
        if not deployment_name:
            logger.error("Missing AZURE_OPENAI_DEPLOYMENT_NAME in environment")
            return []
            
        config_list = [
            {
                "model": deployment_name,
                "api_type": "azure",
                "base_url": os.getenv("AZURE_OPENAI_ENDPOINT"),
                "api_key": os.getenv("AZURE_OPENAI_API_KEY"),
                "api_version": api_version
            }
        ]
    # Fallback to OpenAI
    elif os.getenv("OPENAI_API_KEY"):
        logger.info("Using OpenAI API")
        
        model = os.getenv("OPENAI_MODEL")
        if not model:
            logger.error("Missing OPENAI_MODEL in environment")
            return []
            
        config_list = [
            {
                "model": model,
                "api_key": os.getenv("OPENAI_API_KEY"),
            }
        ]
    else:
        logger.error("No API keys found for OpenAI or Azure OpenAI. Please check your .env file.")
        return []
    
    return config_list

def get_llm_config() -> Dict[str, Any]:
    """
    Create LLM configuration for AutoGen based on environment variables.
    
    Returns:
        LLM configuration dictionary for AutoGen
    """
    config_list = get_config_list()
    if not config_list:
        logger.error("Failed to create config list. Check your environment variables.")
        return {}

    # Read configuration without defaults
    temperature = os.getenv("TEMPERATURE")
    if not temperature:
        logger.error("Missing TEMPERATURE in environment")
        return {}
    
    timeout = os.getenv("TIMEOUT_SECONDS")
    if not timeout:
        logger.error("Missing TIMEOUT_SECONDS in environment")
        return {}
        
    max_tokens = os.getenv("MAX_TOKENS")
    if not max_tokens:
        logger.error("Missing MAX_TOKENS in environment")
        return {}

    try:
        temperature = float(temperature)
        timeout = int(timeout)
        max_tokens = int(max_tokens)
    except ValueError:
        logger.error("Invalid values for TEMPERATURE, TIMEOUT_SECONDS, or MAX_TOKENS. Check your environment variables.")
        return {}

    logger.debug(f"LLM Config: Temperature={temperature}, Timeout={timeout}, Max Tokens={max_tokens}")

    return {
        "config_list": config_list,
        "temperature": temperature,
        "timeout": timeout,
        "max_tokens": max_tokens,
    }