import os
import json
from dotenv import load_dotenv
from typing import Dict, Any

from .logging import get_logger

logger = get_logger(__name__)

# Load environment variables from .env file, if it exists
load_dotenv()

def get_config() -> Dict[str, Any]:
    """Loads configuration from environment variables and JSON files.

    Returns:
        A dictionary containing the configuration.
    """
    config = {}

    # ADLS Configuration
    config['adls'] = {
        'storage_account_name': os.getenv('AZURE_STORAGE_ACCOUNT_NAME'),
        'storage_account_key': os.getenv('AZURE_STORAGE_ACCOUNT_KEY'), # Consider secure alternatives in production
        'container_name': os.getenv('ADLS_CONTAINER_NAME', 'data'),
        'bronze_path': os.getenv('BRONZE_PATH', '/bronze'),
        'silver_path': os.getenv('SILVER_PATH', '/silver'),
        'gold_path': os.getenv('GOLD_PATH', '/gold')
    }

    # SQL Server Configuration
    config['sql_server'] = {
        'host': os.getenv('SQL_SERVER_HOST'),
        'database': os.getenv('SQL_DATABASE_NAME'),
        'user': os.getenv('SQL_USERNAME'),
        'password': os.getenv('SQL_PASSWORD'), # Consider Azure Key Vault in production
        'driver': os.getenv('SQL_DRIVER') # Optional, let Spark auto-detect if None
    }

    # Azure OpenAI Configuration
    config['openai'] = {
        'endpoint': os.getenv('AZURE_OPENAI_ENDPOINT'),
        'api_key': os.getenv('AZURE_OPENAI_API_KEY'), # Consider Azure Key Vault
        'api_version': os.getenv('AZURE_OPENAI_API_VERSION'),
        'deployment_name': os.getenv('AZURE_OPENAI_DEPLOYMENT_NAME')
    }

    # Load LLM parameters from JSON
    llm_config_path = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'sql-to-medallion', 'config', 'llm_config.json')
    try:
        with open(llm_config_path, 'r') as f:
            config['llm'] = json.load(f)
        logger.info(f"Loaded LLM configuration from {llm_config_path}")
    except FileNotFoundError:
        logger.warning(f"LLM configuration file not found at {llm_config_path}. Using defaults.")
        config['llm'] = {"model_parameters": {}, "assistant_instructions": ""}
    except json.JSONDecodeError:
        logger.error(f"Error decoding JSON from {llm_config_path}. Check the file format.")
        config['llm'] = {"model_parameters": {}, "assistant_instructions": ""}

    # Basic validation (add more as needed)
    if not config['adls']['storage_account_name']:
        logger.warning("AZURE_STORAGE_ACCOUNT_NAME is not set.")
    if not config['sql_server']['host']:
        logger.warning("SQL_SERVER_HOST is not set.")
    if not config['openai']['endpoint'] or not config['openai']['api_key']:
        logger.warning("Azure OpenAI endpoint or API key is not fully configured.")

    return config

# Load config once when the module is imported
APP_CONFIG = get_config() 