"""Tests for the agent manager."""

import os
import unittest
from unittest.mock import patch, MagicMock

from src.agents.agent_manager import AgentManager
from src.agents.prompts import get_agent_system_messages


class TestAgentManager(unittest.TestCase):
    """Test cases for the Agent Manager."""
    
    @patch("src.config.llm.get_config_list")
    @patch("autogen.UserProxyAgent")
    @patch("autogen.AssistantAgent")
    @patch("autogen.GroupChat")
    @patch("autogen.GroupChatManager")
    def test_agent_manager_init(self, mock_manager, mock_groupchat, 
                               mock_assistant, mock_user_proxy, mock_get_config):
        """Test AgentManager initialization."""
        # Setup mocks
        mock_get_config.return_value = [{"model": "gpt-4", "api_key": "test-key"}]
        mock_user_proxy.return_value = MagicMock()
        mock_assistant.return_value = MagicMock()
        mock_groupchat.return_value = MagicMock()
        mock_manager.return_value = MagicMock()
        
        # Initialize the agent manager
        agent_manager = AgentManager()
        
        # Check that all agents were created
        self.assertEqual(mock_assistant.call_count, 7)  # 7 specialized agents
        self.assertEqual(mock_user_proxy.call_count, 1)  # 1 user proxy
        
        # Check that the group chat was created
        mock_groupchat.assert_called_once()
        
        # Check that the manager was created
        mock_manager.assert_called_once()
    
    def test_agent_system_messages(self):
        """Test that agent system messages are correctly defined."""
        messages = get_agent_system_messages()
        
        # Check that all expected agents have system messages
        expected_agents = [
            "business_analyst",
            "domain_expert",
            "azure_expert",
            "product_owner",
            "azure_data_engineer",
            "tech_lead",
            "testing_agent"
        ]
        
        for agent in expected_agents:
            self.assertIn(agent, messages)
            self.assertTrue(len(messages[agent]) > 0)
    
    @patch("src.agents.agent_manager.AgentManager._create_agents")
    def test_execute_task(self, mock_create_agents):
        """Test executing a task with the agent manager."""
        # Setup mocks
        agent_manager = AgentManager()
        agent_manager.user_proxy = MagicMock()
        agent_manager.groupchat = MagicMock()
        agent_manager.manager = MagicMock()
        
        # Call execute_task
        task_message = "Test task message"
        results = agent_manager.execute_task(task_message)
        
        # Check that chat was initiated
        agent_manager.user_proxy.initiate_chat.assert_called_once()
        
        # Check results
        self.assertEqual(results["full_conversation"], agent_manager.groupchat.messages)


if __name__ == "__main__":
    unittest.main() 