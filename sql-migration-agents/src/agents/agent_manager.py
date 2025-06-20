"""
Agent manager for SQL Migration Agents.
Handles creating, configuring, and coordinating the agents.
"""

import autogen
from typing import Dict, List, Optional, Any, Union, Callable
from loguru import logger
import os
import re

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
        
        # Track conversation state for orchestration
        self.conversation_state = {
            "phase": "initial",
            "agents_responded": set(),
            "required_agents": set(),
            "task_type": None
        }
        
        # Create the agents
        self._create_agents()
    
    def _create_agents(self):
        """Create all agents in the system."""
        # Create the User Proxy agent to coordinate tasks
        self.user_proxy = autogen.UserProxyAgent(
            name="User",
            human_input_mode="NEVER",
            system_message="I need help migrating SQL Server stored procedures to PySpark for Microsoft Fabric.",
            code_execution_config=False,
            max_consecutive_auto_reply=1  # Limit to avoid loops
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
        
        # Read group chat specific config
        max_iterations = int(os.getenv("MAX_ITERATIONS", "20"))  # Increased default
        logger.debug(f"Group Chat Max Iterations: {max_iterations}")
        
        # Create custom speaker selection function
        speaker_selection_func = self._create_speaker_selection_function()
        
        self.groupchat = autogen.GroupChat(
            agents=self.agents, 
            messages=[],
            max_round=max_iterations,
            speaker_selection_method=speaker_selection_func,
            allow_repeat_speaker=False  # Prevent same agent speaking twice in a row
        )
        
        self.manager = autogen.GroupChatManager(
            groupchat=self.groupchat,
            llm_config=self.llm_config
        )
    
    def _create_speaker_selection_function(self) -> Callable:
        """
        Create a custom speaker selection function for orchestrated conversations.
        
        Returns:
            A function that selects the next speaker based on conversation state
        """
        def select_speaker(last_speaker: autogen.Agent, groupchat: autogen.GroupChat) -> autogen.Agent:
            """Select next speaker based on conversation phase and task type."""
            
            messages = groupchat.messages
            last_message = messages[-1] if messages else None
            
            # Extract speaker name from last message
            if last_message:
                speaker_name = last_message.get("name", last_message.get("sender", ""))
                self.conversation_state["agents_responded"].add(speaker_name)
            
            # Determine task type from initial message
            if self.conversation_state["task_type"] is None and messages:
                first_msg = messages[0].get("content", "")
                if "analyze" in first_msg.lower():
                    self.conversation_state["task_type"] = "analysis"
                    self.conversation_state["required_agents"] = {
                        "BusinessAnalyst", "DomainExpert", "AzureExpert"
                    }
                elif "migrate" in first_msg.lower():
                    self.conversation_state["task_type"] = "migration"
                    self.conversation_state["required_agents"] = {
                        "BusinessAnalyst", "ProductOwner", "AzureDataEngineer", 
                        "TechLead", "TestingAgent"
                    }
            
            # Analysis task flow
            if self.conversation_state["task_type"] == "analysis":
                if "BusinessAnalyst" not in self.conversation_state["agents_responded"]:
                    return self.business_analyst
                elif "DomainExpert" not in self.conversation_state["agents_responded"]:
                    return self.domain_expert
                elif "AzureExpert" not in self.conversation_state["agents_responded"]:
                    return self.azure_expert
                else:
                    # All required agents have responded, terminate
                    return None
            
            # Migration task flow
            elif self.conversation_state["task_type"] == "migration":
                # Phase 1: Business Analysis
                if "BusinessAnalyst" not in self.conversation_state["agents_responded"]:
                    return self.business_analyst
                
                # Phase 2: Planning
                elif "ProductOwner" not in self.conversation_state["agents_responded"]:
                    return self.product_owner
                
                # Phase 3: Implementation
                elif "AzureDataEngineer" not in self.conversation_state["agents_responded"]:
                    return self.azure_data_engineer
                
                # Phase 4: Review
                elif "TechLead" not in self.conversation_state["agents_responded"]:
                    # Check if AzureDataEngineer has provided code
                    azure_response = extract_response(messages, "AzureDataEngineer")
                    if azure_response and ("```python" in azure_response or "def " in azure_response):
                        return self.tech_lead
                    else:
                        # No code to review yet, skip TechLead
                        self.conversation_state["agents_responded"].add("TechLead")
                
                # Phase 5: Testing
                elif "TestingAgent" not in self.conversation_state["agents_responded"]:
                    return self.testing_agent
                
                else:
                    # All required agents have responded, terminate
                    return None
            
            # Default: Use autogen's default selection
            return "auto"
        
        return select_speaker
    
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
        
        # Reset conversation state
        self.conversation_state = {
            "phase": "initial",
            "agents_responded": set(),
            "required_agents": set(),
            "task_type": None
        }
        
        try:
            # Initiate the chat
            self.user_proxy.initiate_chat(
                self.manager,
                message=task_message,
                clear_history=True,
                silent=False
            )
            
            # Get the messages from the groupchat object
            final_messages = self.groupchat.messages
            message_count = len(final_messages)
            logger.info(f"Chat completed. Found {message_count} messages in groupchat object.")
            
            # Verify all required agents responded
            missing_agents = self.conversation_state["required_agents"] - self.conversation_state["agents_responded"]
            if missing_agents:
                logger.warning(f"Missing responses from agents: {missing_agents}")

            # Return the messages found directly in the groupchat
            return {
                "full_conversation": final_messages,
                "success": True,
                "agents_responded": list(self.conversation_state["agents_responded"]),
                "missing_agents": list(missing_agents) if missing_agents else []
            }
        except Exception as e:
            # Log detailed error information
            logger.error(f"Detailed error during chat initiation: {str(e)}")
            logger.error(f"Error type: {type(e).__name__}")
            if hasattr(e, '__cause__') and e.__cause__:
                logger.error(f"Caused by: {str(e.__cause__)}")
            
            # Return an error response
            return {
                "full_conversation": [],
                "error": str(e),
                "success": False
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
                "businessanalyst": self.business_analyst,
                "domainexpert": self.domain_expert,
                "azureexpert": self.azure_expert,
                "productowner": self.product_owner,
                "azuredataengineer": self.azure_data_engineer,
                "techlead": self.tech_lead,
                "testingagent": self.testing_agent
            }
            
            # Normalize agent name
            normalized_name = agent_name.lower().replace("_", "").replace("-", "")
            
            agent = agent_map.get(normalized_name)
            if not agent:
                available_agents = ", ".join(agent_map.keys())
                raise ValueError(f"Unknown agent: {agent_name}. Available agents: {available_agents}")
            
            # Create a simple 1:1 chat for direct interaction
            chat = autogen.GroupChat(
                agents=[self.user_proxy, agent],
                messages=[],
                max_round=2  # Just one exchange
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
                msg_name = m.get("name", m.get("sender", ""))
                if msg_name == agent.name:
                    return m.get("content", "No response content found")
            
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
        # Debug logging
        logger.debug(f"Extracting analysis results from {len(messages)} messages")
        
        # Use the provided messages list
        business_analysis = extract_response(messages, "BusinessAnalyst")
        technical_analysis = extract_response(messages, "DomainExpert")
        azure_recommendations = extract_response(messages, "AzureExpert")
        
        # Fallback Logic with better logging
        if not business_analysis:
            logger.warning("No BusinessAnalyst response found. Trying ProductOwner...")
            business_analysis = extract_response(messages, "ProductOwner")
            if not business_analysis:
                logger.warning("No BusinessAnalyst or ProductOwner response found.")
                business_analysis = "No business analysis available."

        if not technical_analysis:
            logger.warning("No DomainExpert response found. Trying TechLead or AzureDataEngineer...")
            technical_analysis = extract_response(messages, "TechLead")
            if not technical_analysis:
                technical_analysis = extract_response(messages, "AzureDataEngineer")
            if not technical_analysis:
                logger.warning("No DomainExpert, TechLead, or AzureDataEngineer response found.")
                technical_analysis = "No technical analysis available."

        if not azure_recommendations:
            logger.warning("No AzureExpert response found. Trying AzureDataEngineer...")
            azure_recommendations = extract_response(messages, "AzureDataEngineer")
            if not azure_recommendations:
                logger.warning("No AzureExpert or AzureDataEngineer response found.")
                azure_recommendations = "No Azure recommendations available."

        # Structure the results
        return {
            "business_analysis": business_analysis, 
            "technical_analysis": technical_analysis,
            "azure_recommendations": azure_recommendations,
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
        # Debug logging
        logger.debug(f"Extracting migration results from {len(messages)} messages")
        
        # Extract PySpark code from the Azure Data Engineer's response
        azure_response = extract_response(messages, "AzureDataEngineer")
        pyspark_code = self._extract_labeled_code_blocks(azure_response)
        
        # If no labeled blocks found, fall back to regular code extraction
        if not pyspark_code:
            logger.warning("No labeled code blocks found, using generic extraction")
            pyspark_code = extract_code_blocks(azure_response)
        
        # Extract test cases from the Testing Agent's response
        test_response = extract_response(messages, "TestingAgent")
        test_cases = extract_code_blocks(test_response) if test_response else []
        
        # Extract migration plan from the Product Owner's response
        migration_plan = extract_response(messages, "ProductOwner")
        if not migration_plan:
            migration_plan = "No migration plan available."
        
        return {
            "pyspark_code": pyspark_code,
            "test_cases": test_cases,
            "migration_plan": migration_plan,
        }
    
    def _extract_labeled_code_blocks(self, text: str) -> List[str]:
        """
        Extract code blocks with specific labels for different layers.
        
        Args:
            text: Text containing labeled code blocks
            
        Returns:
            List of labeled code blocks
        """
        if not text:
            return []
        
        # Define patterns for different layers
        layer_patterns = [
            r'# BRONZE LAYER START',
            r'# STAGE 1: BASE DATA START',
            r'# STAGE 2: ADVANCED ANALYTICS START',
            r'# GOLD LAYER START',
            r'# SILVER LAYER START'  # For backward compatibility
        ]
        
        labeled_blocks = []
        
        # Split text into code blocks
        code_blocks = []
        lines = text.split('\n')
        in_code_block = False
        current_block = []
        block_language = ""
        
        for line in lines:
            if line.strip().startswith("```"):
                if not in_code_block:
                    in_code_block = True
                    # Extract language if present
                    lang_match = re.match(r"```(\w+)", line.strip())
                    if lang_match:
                        block_language = lang_match.group(1)
                else:
                    in_code_block = False
                    if current_block:
                        # Check if this block contains any layer pattern
                        block_content = "\n".join(current_block)
                        for pattern in layer_patterns:
                            if pattern in block_content:
                                # Include the language header if it's python
                                if block_language.lower() == "python":
                                    labeled_blocks.append(f"```python\n{block_content}\n```")
                                else:
                                    labeled_blocks.append(block_content)
                                break
                        else:
                            # No specific label found, but still include if it's python code
                            if block_language.lower() == "python" and current_block:
                                code_blocks.append(f"```python\n{block_content}\n```")
                    current_block = []
                    block_language = ""
            elif in_code_block:
                current_block.append(line)
        
        # Return labeled blocks if found, otherwise return all python blocks
        return labeled_blocks if labeled_blocks else code_blocks