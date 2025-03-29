import autogen
from typing import Dict, List, Optional, Any
from loguru import logger
import os # Need os to read env vars here

from src.config.llm import get_llm_config

class AgentManager:
    def __init__(self):
        self.groupchat = None
        self.llm_config = None
        self.manager = None

    def _create_agents(self, max_iterations):
        self.groupchat = autogen.GroupChat(
            max_round=max_iterations # Use the configured value
        )

        self.manager = autogen.GroupChatManager(
            groupchat=self.groupchat,
            llm_config=self.llm_config
            # Consider adding system_message to the manager for overall guidance
            # system_message=os.getenv("GROUP_CHAT_SYSTEM_MESSAGE", "Manage the group chat effectively.")
        )

    # ... (rest of the AgentManager code) ... 