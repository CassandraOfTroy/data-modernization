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
        logger.error("No API keys found for OpenAI or Azure OpenAI")
        config_list = []
    
    return config_list

def get_llm_config() -> Dict[str, Any]:
    """
    Create LLM configuration for AutoGen based on environment variables.
    
    Returns:
        LLM configuration dictionary for AutoGen
    """
    config_list = get_config_list()
    
    return {
        "config_list": config_list,
        "temperature": float(os.getenv("TEMPERATURE", "0.2")),
        "timeout": int(os.getenv("TIMEOUT_SECONDS", "120"))
    } 