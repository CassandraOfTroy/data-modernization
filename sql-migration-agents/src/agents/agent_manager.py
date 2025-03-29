"""
Agent manager for SQL Migration Agents.
Handles creating, configuring, and coordinating the agents.
"""

import autogen
from typing import Dict, List, Optional, Any
from loguru import logger

from src.config.llm import get_llm_config
from src.agents.prompts import get_agent_system_messages
from src.agents.utils import extract_response, extract_code_blocks

class AgentManager:
    """
    Manager class for the SQL Migration Agents.
    Creates and coordinates the agents for SQL migration tasks.
    """
    
    def __init__(self, llm_config: Optional[Dict[str, Any]] = None):
        """
        Initialize the agent manager.
        
        Args:
            llm_config: Optional LLM configuration override
        """
        # Get LLM configuration
        self.llm_config = llm_config or get_llm_config()
        
        # Get agent system messages
        self.system_messages = get_agent_system_messages()
        
        # Create the agents
        self._create_agents()
    
    def _create_agents(self):
        """Create all agents in the system."""
        # Create the User Proxy agent to coordinate tasks
        self.user_proxy = autogen.UserProxyAgent(
            name="User",
            human_input_mode="NEVER",
            system_message="I need help migrating SQL Server stored procedures to PySpark for Microsoft Fabric.",
            code_execution_config={"use_docker": False},
        )
        
        # Create specialized agents
        self.business_analyst = autogen.AssistantAgent(
            name="BusinessAnalyst",
            system_message=self.system_messages["business_analyst"],
            llm_config=self.llm_config
        )
        
        self.domain_expert = autogen.AssistantAgent(
            name="DomainExpert",
            system_message=self.system_messages["domain_expert"],
            llm_config=self.llm_config
        )
        
        self.azure_expert = autogen.AssistantAgent(
            name="AzureExpert",
            system_message=self.system_messages["azure_expert"],
            llm_config=self.llm_config
        )
        
        self.product_owner = autogen.AssistantAgent(
            name="ProductOwner",
            system_message=self.system_messages["product_owner"],
            llm_config=self.llm_config
        )
        
        self.azure_data_engineer = autogen.AssistantAgent(
            name="AzureDataEngineer",
            system_message=self.system_messages["azure_data_engineer"],
            llm_config=self.llm_config
        )
        
        self.tech_lead = autogen.AssistantAgent(
            name="TechLead",
            system_message=self.system_messages["tech_lead"],
            llm_config=self.llm_config
        )
        
        self.testing_agent = autogen.AssistantAgent(
            name="TestingAgent",
            system_message=self.system_messages["testing_agent"],
            llm_config=self.llm_config
        )
        
        # Create group chat for collaboration
        self.agents = [
            self.user_proxy,
            self.business_analyst,
            self.domain_expert, 
            self.azure_expert,
            self.product_owner,
            self.azure_data_engineer,
            self.tech_lead,
            self.testing_agent
        ]
        
        self.groupchat = autogen.GroupChat(
            agents=self.agents, 
            messages=[],
            max_round=10
        )
        
        self.manager = autogen.GroupChatManager(
            groupchat=self.groupchat,
            llm_config=self.llm_config
        )
    
    def execute_task(self, task_message: str) -> Dict[str, Any]:
        """
        Execute a generic task with the agent group.
        
        Args:
            task_message: The task message to send to the agents
            
        Returns:
            The raw group chat messages
        """
        logger.info("Executing task with agent group")
        
        # Reset the chat
        self.groupchat.messages = []
        
        # Initiate the chat
        self.user_proxy.initiate_chat(
            self.manager,
            message=task_message
        )
        
        return {
            "full_conversation": self.groupchat.messages
        }
    
    def interact_with_agent(self, agent_name: str, message: str) -> str:
        """
        Interact with a specific agent directly.
        
        Args:
            agent_name: Name of the agent to interact with
            message: Message to send to the agent
            
        Returns:
            Agent's response
        """
        logger.info(f"Interacting with agent: {agent_name}")
        
        try:
            # Map agent name to the actual agent object
            agent_map = {
                "business_analyst": self.business_analyst,
                "domain_expert": self.domain_expert,
                "azure_expert": self.azure_expert,
                "product_owner": self.product_owner,
                "azure_data_engineer": self.azure_data_engineer,
                "tech_lead": self.tech_lead,
                "testing_agent": self.testing_agent
            }
            
            agent = agent_map.get(agent_name.lower())
            if not agent:
                raise ValueError(f"Unknown agent: {agent_name}")
            
            # Create a simple 1:1 chat for direct interaction
            chat = autogen.GroupChat(
                agents=[self.user_proxy, agent],
                messages=[]
            )
            manager = autogen.GroupChatManager(
                groupchat=chat,
                llm_config=self.llm_config
            )
            
            # Initiate the chat
            self.user_proxy.initiate_chat(
                manager,
                message=message
            )
            
            # Extract and return the agent's response
            for m in reversed(chat.messages):
                if m["sender"] == agent.name:
                    return m["content"]
            
            return "No response received from agent."
            
        except Exception as e:
            logger.error(f"Error interacting with agent: {str(e)}")
            raise
    
    def get_results_for_analysis(self) -> Dict[str, Any]:
        """
        Extract results from the group chat for an analysis task.
        Should be called after execute_task() for an analysis task.
        
        Returns:
            Dictionary of analysis results by agent
        """
        return {
            "business_analysis": extract_response(self.groupchat.messages, "BusinessAnalyst"),
            "technical_analysis": extract_response(self.groupchat.messages, "DomainExpert"),
            "azure_recommendations": extract_response(self.groupchat.messages, "AzureExpert"),
            "full_conversation": self.groupchat.messages
        }
    
    def get_results_for_migration(self) -> Dict[str, Any]:
        """
        Extract results from the group chat for a migration task.
        Should be called after execute_task() for a migration task.
        
        Returns:
            Dictionary of migration artifacts by type
        """
        # Extract PySpark code from the Azure Data Engineer's response
        pyspark_code = extract_code_blocks(
            extract_response(self.groupchat.messages, "AzureDataEngineer")
        )
        
        # Extract test cases from the Testing Agent's response
        test_cases = extract_code_blocks(
            extract_response(self.groupchat.messages, "TestingAgent")
        )
        
        # Extract migration plan from the Product Owner's response
        migration_plan = extract_response(self.groupchat.messages, "ProductOwner")
        
        return {
            "pyspark_code": pyspark_code,
            "test_cases": test_cases,
            "migration_plan": migration_plan,
            "full_conversation": self.groupchat.messages
        } 