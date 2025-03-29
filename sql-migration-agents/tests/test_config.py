"""Tests for the configuration modules."""

import os
import unittest
from unittest.mock import patch

from src.config.llm import get_config_list, get_llm_config


class TestLLMConfig(unittest.TestCase):
    """Test cases for the LLM configuration module."""
    
    @patch.dict(os.environ, {
        "AZURE_OPENAI_API_KEY": "test-key",
        "AZURE_OPENAI_ENDPOINT": "https://test-endpoint.com",
        "AZURE_OPENAI_DEPLOYMENT_NAME": "test-deployment",
        "AZURE_OPENAI_API_VERSION": "2023-12-01-preview"
    })
    def test_get_config_list_azure(self):
        """Test that get_config_list correctly uses Azure OpenAI config."""
        config_list = get_config_list()
        
        self.assertEqual(len(config_list), 1)
        config = config_list[0]
        
        self.assertEqual(config["api_key"], "test-key")
        self.assertEqual(config["api_base"], "https://test-endpoint.com")
        self.assertEqual(config["model"], "test-deployment")
        self.assertEqual(config["api_type"], "azure")
        self.assertEqual(config["api_version"], "2023-12-01-preview")
    
    @patch.dict(os.environ, {
        "OPENAI_API_KEY": "test-key",
        "OPENAI_MODEL": "gpt-4"
    }, clear=True)
    def test_get_config_list_openai(self):
        """Test that get_config_list correctly uses OpenAI config."""
        config_list = get_config_list()
        
        self.assertEqual(len(config_list), 1)
        config = config_list[0]
        
        self.assertEqual(config["api_key"], "test-key")
        self.assertEqual(config["model"], "gpt-4")
    
    @patch("src.config.llm.get_config_list")
    @patch.dict(os.environ, {
        "TEMPERATURE": "0.5",
        "TIMEOUT_SECONDS": "60"
    })
    def test_get_llm_config(self, mock_get_config_list):
        """Test that get_llm_config correctly builds the configuration."""
        mock_config_list = [{"model": "gpt-4", "api_key": "test-key"}]
        mock_get_config_list.return_value = mock_config_list
        
        llm_config = get_llm_config()
        
        self.assertEqual(llm_config["config_list"], mock_config_list)
        self.assertEqual(llm_config["temperature"], 0.5)
        self.assertEqual(llm_config["timeout"], 60)


if __name__ == "__main__":
    unittest.main() 