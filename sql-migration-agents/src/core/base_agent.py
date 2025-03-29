import os
from typing import Dict, List, Optional, Any
import openai
from openai import AzureOpenAI
from loguru import logger
from pydantic import BaseModel, Field
import time
import asyncio
import sys

# Optional imports - will be used only if the services are enabled
try:
    from azure.identity import DefaultAzureCredential, ClientSecretCredential
    from azure.keyvault.secrets import SecretClient
    AZURE_IMPORTS_AVAILABLE = True
except ImportError:
    AZURE_IMPORTS_AVAILABLE = False
    logger.warning("Azure SDK not fully available. Install with: pip install azure-identity azure-keyvault-secrets")

try:
    from applicationinsights import TelemetryClient
    from applicationinsights.logging import LoggingHandler
    APP_INSIGHTS_AVAILABLE = True
except ImportError:
    APP_INSIGHTS_AVAILABLE = False
    logger.warning("Application Insights not available. Install with: pip install applicationinsights")

class Message(BaseModel):
    role: str
    content: str

class AgentResponse(BaseModel):
    response: str
    details: Optional[Dict[str, Any]] = None
    success: bool = True
    error: Optional[str] = None

class BaseAgent:
    """Base agent class that all specialized agents inherit from"""
    
    def __init__(self, name: str, description: str, system_prompt: str):
        """Initialize the agent with name, description and system prompt"""
        self.name = name
        self.description = description
        self.system_prompt = system_prompt
        self.conversation_history: List[Message] = []
        
        # Get configuration from environment variables
        self.use_key_vault = os.getenv("USE_KEY_VAULT", "false").lower() == "true"
        self.enable_app_insights = os.getenv("ENABLE_APP_INSIGHTS", "false").lower() == "true"
        
        # Configure model parameters
        self.azure_openai_model = os.getenv("AZURE_OPENAI_MODEL", "gpt-4")
        self.temperature = float(os.getenv("TEMPERATURE", "0.2"))
        self.max_tokens = int(os.getenv("MAX_TOKENS", "4000"))
        self.max_retries = int(os.getenv("MAX_RETRIES", "3"))
        self.retry_delay = float(os.getenv("RETRY_DELAY", "1.0"))
        
        # Set up Application Insights if enabled
        if self.enable_app_insights and APP_INSIGHTS_AVAILABLE:
            self._setup_app_insights()
        
        # Initialize Azure OpenAI client
        self._initialize_azure_openai_client()
    
    def _setup_app_insights(self):
        """Set up Azure Application Insights for telemetry"""
        try:
            connection_string = os.getenv("APPLICATIONINSIGHTS_CONNECTION_STRING")
            if connection_string:
                self.telemetry_client = TelemetryClient(connection_string)
                # Add Application Insights logging handler
                handler = LoggingHandler(connection_string)
                logger.add(handler)
                logger.info(f"Application Insights enabled for agent {self.name}")
            else:
                logger.warning(f"Application Insights connection string not found for agent {self.name}")
                self.telemetry_client = None
        except Exception as e:
            logger.error(f"Error setting up Application Insights: {e}")
            self.telemetry_client = None
    
    def _get_secret_from_keyvault(self, secret_name: str) -> Optional[str]:
        """Get a secret from Azure Key Vault"""
        try:
            if not AZURE_IMPORTS_AVAILABLE:
                logger.error("Azure Key Vault imports not available")
                return None
                
            vault_name = os.getenv("AZURE_KEY_VAULT_NAME")
            if not vault_name:
                logger.error("Azure Key Vault name not specified")
                return None
                
            # Try to use Default Azure Credential first
            try:
                credential = DefaultAzureCredential()
            except Exception:
                # Fall back to Client Secret Credential
                tenant_id = os.getenv("AZURE_TENANT_ID")
                client_id = os.getenv("AZURE_CLIENT_ID") 
                client_secret = os.getenv("AZURE_CLIENT_SECRET")
                
                if not all([tenant_id, client_id, client_secret]):
                    logger.error("Azure credentials not fully specified")
                    return None
                    
                credential = ClientSecretCredential(
                    tenant_id=tenant_id,
                    client_id=client_id,
                    client_secret=client_secret
                )
            
            # Get the secret from Key Vault
            vault_url = f"https://{vault_name}.vault.azure.net/"
            client = SecretClient(vault_url=vault_url, credential=credential)
            secret = client.get_secret(secret_name)
            logger.info(f"Retrieved secret {secret_name} from Key Vault")
            return secret.value
            
        except Exception as e:
            logger.error(f"Error retrieving secret from Key Vault: {e}")
            return None
    
    def _initialize_azure_openai_client(self):
        """Initialize the Azure OpenAI client"""
        try:
            # Get API key - either directly from env vars or from Key Vault
            if self.use_key_vault and AZURE_IMPORTS_AVAILABLE:
                api_key = self._get_secret_from_keyvault("azure-openai-api-key")
            else:
                api_key = os.getenv("AZURE_OPENAI_API_KEY")
                
            endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
            api_version = os.getenv("AZURE_OPENAI_API_VERSION", "2023-12-01-preview")
            
            if api_key and endpoint:
                self.client = AzureOpenAI(
                    api_key=api_key,
                    api_version=api_version,
                    azure_endpoint=endpoint
                )
                logger.info(f"Initialized Azure OpenAI client for agent {self.name}")
            else:
                missing = []
                if not api_key: missing.append("API key")
                if not endpoint: missing.append("endpoint")
                logger.error(f"Azure OpenAI {' and '.join(missing)} not found for agent {self.name}")
                self.client = None
        except Exception as e:
            logger.error(f"Error initializing Azure OpenAI client for agent {self.name}: {e}")
            self.client = None
    
    def reset_conversation(self):
        """Reset the conversation history"""
        self.conversation_history = []
        logger.debug(f"Reset conversation history for agent {self.name}")
    
    def add_message(self, role: str, content: str):
        """Add a message to the conversation history"""
        self.conversation_history.append(Message(role=role, content=content))
        
    def get_messages(self) -> List[Dict[str, str]]:
        """Get all messages in the format expected by OpenAI"""
        messages = [{"role": "system", "content": self.system_prompt}]
        messages.extend([{"role": msg.role, "content": msg.content} for msg in self.conversation_history])
        return messages
    
    def _handle_error(self, error: Exception) -> AgentResponse:
        """Handle errors and return appropriate response"""
        if isinstance(error, openai.APIError):
            logger.error(f"OpenAI API error in agent {self.name}: {str(error)}")
            return AgentResponse(
                response=f"Error from Azure OpenAI API: {str(error)}",
                success=False,
                error=str(error)
            )
        elif isinstance(error, openai.APIConnectionError):
            logger.error(f"Connection error in agent {self.name}: {str(error)}")
            return AgentResponse(
                response=f"Failed to connect to Azure OpenAI API: {str(error)}",
                success=False,
                error=str(error)
            )
        elif isinstance(error, openai.RateLimitError):
            logger.error(f"Rate limit error in agent {self.name}: {str(error)}")
            return AgentResponse(
                response=f"Azure OpenAI API rate limit exceeded: {str(error)}",
                success=False,
                error=str(error)
            )
        else:
            logger.error(f"Unexpected error in agent {self.name}: {str(error)}")
            return AgentResponse(
                response=f"Error processing request: {str(error)}",
                success=False,
                error=str(error)
            )
    
    async def process(self, user_input: str) -> AgentResponse:
        """Process user input and return a response"""
        if not self.client:
            logger.error(f"Azure OpenAI client not initialized for agent {self.name}")
            return AgentResponse(
                response="Azure OpenAI client not initialized. Please check your configuration.",
                success=False,
                error="Azure OpenAI client not initialized"
            )
        
        start_time = time.time()
        
        try:
            # Add user input to conversation history
            self.add_message("user", user_input)
            logger.debug(f"Processing input for agent {self.name}: {user_input[:50]}...")
            
            # Get response from Azure OpenAI
            logger.debug(f"Sending request to Azure OpenAI with model {self.azure_openai_model}")
            response = await self._get_completion_with_retry()
            
            # Extract the response text
            response_text = response.choices[0].message.content
            
            # Add assistant response to conversation history
            self.add_message("assistant", response_text)
            logger.debug(f"Received response from Azure OpenAI for agent {self.name}: {response_text[:50]}...")
            
            # Track telemetry if Application Insights is enabled
            if self.enable_app_insights and hasattr(self, 'telemetry_client') and self.telemetry_client:
                elapsed_time = time.time() - start_time
                properties = {
                    "agent_name": self.name,
                    "model": self.azure_openai_model,
                    "success": "true"
                }
                metrics = {"elapsed_time_seconds": elapsed_time}
                self.telemetry_client.track_event("AgentProcessing", properties, metrics)
                self.telemetry_client.flush()
            
            return AgentResponse(response=response_text)
            
        except Exception as e:
            # Track error telemetry
            if self.enable_app_insights and hasattr(self, 'telemetry_client') and self.telemetry_client:
                properties = {
                    "agent_name": self.name,
                    "model": self.azure_openai_model,
                    "success": "false",
                    "error": str(e)
                }
                self.telemetry_client.track_exception(*sys.exc_info(), properties=properties)
                self.telemetry_client.flush()
                
            return self._handle_error(e)
    
    async def _get_completion_with_retry(self):
        """Get completion from Azure OpenAI with retry logic"""
        deployment_name = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME")
        
        retries = 0
        while retries <= self.max_retries:
            try:
                return self.client.chat.completions.create(
                    model=deployment_name,
                    messages=self.get_messages(),
                    temperature=self.temperature,
                    max_tokens=self.max_tokens
                )
            except (openai.APIError, openai.APIConnectionError, openai.RateLimitError) as e:
                retries += 1
                if retries <= self.max_retries:
                    wait_time = self.retry_delay * (2 ** (retries - 1))  # Exponential backoff
                    logger.warning(f"Error calling Azure OpenAI API: {str(e)}. Retrying in {wait_time:.1f}s ({retries}/{self.max_retries})")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"Failed after {self.max_retries} retries: {str(e)}")
                    raise
            except Exception as e:
                logger.error(f"Unexpected error getting completion: {str(e)}")
                raise 