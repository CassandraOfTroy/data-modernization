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
            # Disable code execution entirely
            code_execution_config=None # Disable code execution
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
            Dictionary containing the full conversation history.
        """
        logger.info("Executing task with agent group")
        
        # Reset the group chat state before starting
        self.groupchat.reset()
        
        # Initiate the chat (messages will be stored in self.groupchat.messages)
        self.user_proxy.initiate_chat(
            self.manager,
            message=task_message,
            clear_history=True, # Recommended to keep True for clean runs
            silent=False # Keep False to see console logs
        )
        
        # Immediately get the messages from the groupchat object
        final_messages = self.groupchat.messages
        message_count = len(final_messages)
        logger.info(f"Chat completed. Found {message_count} messages in groupchat object.")

        # Return the messages found directly in the groupchat
        return {
            "full_conversation": final_messages
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
    
    def get_results_for_analysis(self, messages: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Extract results from the provided message list for an analysis task.
        Should be called after execute_task() for an analysis task.
        
        Returns:
            Dictionary of analysis results by agent
        """
        # --- DEBUGGING: Print last few messages to check structure ---
        if messages:
            print("\n--- DEBUG: Checking message structure in get_results_for_analysis ---")
            num_messages_to_print = min(len(messages), 5) # Print up to last 5 messages
            for i in range(num_messages_to_print):
                msg_index = -(i + 1)
                message = messages[msg_index]
                print(f"Message {msg_index}:")
                if isinstance(message, dict):
                    print(f"  Keys: {message.keys()}")
                    print(f"  -> name: {message.get('name')}")
                    print(f"  -> sender: {message.get('sender')}")
                    print(f"  -> role: {message.get('role')}")
                    content_snippet = str(message.get('content', ''))[:100].replace('\n', ' ') + "..."
                    print(f"  -> content: {content_snippet}")
                else:
                    print(f"  Type: {type(message)}")
                    print(f"  Value: {str(message)[:100]}...")
            print("--- END DEBUG ---\n")
        # --- END DEBUGGING ---
        
        # Use the provided messages list
        business_analysis = extract_response(messages, "BusinessAnalyst")
        technical_analysis = extract_response(messages, "DomainExpert")
        azure_recommendations = extract_response(messages, "AzureExpert")
        
        # --- Re-introducing Fallback Logic --- 
        if not business_analysis:
            logger.warning("No BusinessAnalyst response found. Trying ProductOwner...")
            business_analysis = extract_response(messages, "ProductOwner")
            if not business_analysis:
                 logger.warning("No BusinessAnalyst or ProductOwner response found.")
                 # Keep business_analysis as potentially empty string if not found

        if not technical_analysis:
            logger.warning("No DomainExpert response found. Trying TechLead or AzureDataEngineer...")
            technical_analysis = extract_response(messages, "TechLead")
            if not technical_analysis:
                technical_analysis = extract_response(messages, "AzureDataEngineer")
            if not technical_analysis:
                logger.warning("No DomainExpert, TechLead, or AzureDataEngineer response found.")
                # Keep technical_analysis as potentially empty string

        if not azure_recommendations:
            logger.warning("No AzureExpert response found. Trying AzureDataEngineer...")
            azure_recommendations = extract_response(messages, "AzureDataEngineer")
            if not azure_recommendations:
                logger.warning("No AzureExpert or AzureDataEngineer response found.")
                # Keep azure_recommendations as potentially empty string
        # --- End Fallback Logic --- 

        # Structure the results, ensuring they are always strings
        return {
            "business_analysis": business_analysis or "", 
            "technical_analysis": technical_analysis or "",
            "azure_recommendations": azure_recommendations or "",
            # Removed "full_conversation" as it was redundant with the input 'messages'
        }
    
    def get_results_for_migration(self, messages: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Extract results from the provided message list for a migration task.
        Should be called after execute_task() for a migration task.

        Args:
            messages: The list of message dictionaries from the conversation.
            
        Returns:
            Dictionary of migration artifacts by type
        """
        # Extract PySpark code from the Azure Data Engineer's response using the passed messages
        pyspark_code = extract_code_blocks(
            extract_response(messages, "AzureDataEngineer")
        )
        
        # Extract test cases from the Testing Agent's response using the passed messages
        test_cases = extract_code_blocks(
            extract_response(messages, "TestingAgent")
        )
        
        # Extract migration plan from the Product Owner's response using the passed messages
        migration_plan = extract_response(messages, "ProductOwner")
        
        return {
            "pyspark_code": pyspark_code,
            "test_cases": test_cases,
            "migration_plan": migration_plan,
            # Removed "full_conversation" as it's redundant with the input messages
        } 