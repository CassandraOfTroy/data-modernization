import os
from typing import Dict, List, Optional, Any
from loguru import logger

from src.core.base_agent import BaseAgent, AgentResponse
from src.agents.business_analyst import BusinessAnalystAgent
from src.agents.domain_expert import DomainExpertAgent
from src.agents.azure_expert import AzureCloudExpertAgent
from src.agents.product_owner import ProductOwnerAgent
from src.agents.azure_data_engineer import AzureDataEngineerAgent
from src.agents.tech_lead import TechLeadAgent
from src.agents.testing_agent import TestingAgent

class AgentManager:
    """
    Manages and coordinates all specialized agents in the system.
    """
    
    def __init__(self):
        """Initialize the agent manager with all specialized agents"""
        # Initialize all agents
        self.agents: Dict[str, BaseAgent] = {
            "business_analyst": BusinessAnalystAgent(),
            "domain_expert": DomainExpertAgent(),
            "azure_expert": AzureCloudExpertAgent(),
            "product_owner": ProductOwnerAgent(),
            "azure_data_engineer": AzureDataEngineerAgent(),
            "tech_lead": TechLeadAgent(),
            "testing_agent": TestingAgent()
        }
        
        logger.info(f"Initialized agent manager with {len(self.agents)} agents")
    
    def get_agent(self, agent_name: str) -> Optional[BaseAgent]:
        """Get an agent by name"""
        if agent_name in self.agents:
            return self.agents[agent_name]
        else:
            logger.warning(f"Agent '{agent_name}' not found")
            return None
    
    def list_agents(self) -> List[Dict[str, str]]:
        """List all available agents with their names and descriptions"""
        return [
            {"name": name, "description": agent.description}
            for name, agent in self.agents.items()
        ]
    
    async def run_migration_workflow(self, sql_code: str, business_context: str = "") -> Dict[str, Any]:
        """
        Execute a full migration workflow involving all agents
        
        Args:
            sql_code: The SQL code to migrate
            business_context: Business context about the SQL code
            
        Returns:
            Results from the full migration workflow
        """
        results = {}
        
        # Step 1: Business Analyst analyzes the SQL procedure
        logger.info("Starting business analysis")
        business_analysis = await self.agents["business_analyst"].analyze_sql_procedure(
            sql_code=sql_code,
            business_context=business_context
        )
        results["business_analysis"] = business_analysis.response
        
        # Step 2: Domain Expert provides deep technical analysis
        logger.info("Starting domain expert analysis")
        domain_analysis = await self.agents["domain_expert"].analyze_sql_details(sql_code)
        results["domain_analysis"] = domain_analysis.response
        
        # Step 3: Create business requirements
        logger.info("Creating business requirements")
        business_requirements = await self.agents["business_analyst"].create_business_requirements(
            {"analysis": business_analysis.response, "technical_details": domain_analysis.response}
        )
        results["business_requirements"] = business_requirements.response
        
        # Step 4: Azure Expert recommends architecture
        logger.info("Getting architecture recommendations")
        architecture = await self.agents["azure_expert"].recommend_azure_architecture(
            requirements=business_requirements.response
        )
        results["architecture"] = architecture.response
        
        # Step 5: Product Owner creates user stories
        logger.info("Creating user stories")
        user_stories = await self.agents["product_owner"].create_user_stories(
            business_requirements=business_requirements.response,
            technical_analysis=domain_analysis.response
        )
        results["user_stories"] = user_stories.response
        
        # Step 6: Azure Data Engineer translates the SQL procedure
        logger.info("Translating SQL to PySpark")
        translated_code = await self.agents["azure_data_engineer"].translate_full_procedure(
            sql_procedure=sql_code,
            additional_context=f"{business_analysis.response}\n\n{domain_analysis.response}"
        )
        results["translated_code"] = translated_code.response
        
        # Step 7: Tech Lead reviews the code
        logger.info("Reviewing code")
        code_review = await self.agents["tech_lead"].review_code(
            code=translated_code.response,
            context=f"SQL to PySpark migration for {business_context}"
        )
        results["code_review"] = code_review.response
        
        # Step 8: Azure Data Engineer refactors based on feedback
        logger.info("Refactoring code based on review")
        refactored_code = await self.agents["azure_data_engineer"].translate_full_procedure(
            sql_procedure=sql_code,
            additional_context=f"{business_analysis.response}\n\n{domain_analysis.response}\n\nREVIEW FEEDBACK:\n{code_review.response}"
        )
        results["refactored_code"] = refactored_code.response
        
        # Step 9: Testing Agent creates a test plan
        logger.info("Creating test plan")
        test_plan = await self.agents["testing_agent"].create_test_plan(
            sql_code=sql_code,
            pyspark_code=refactored_code.response,
            requirements=business_requirements.response
        )
        results["test_plan"] = test_plan.response
        
        logger.info("Migration workflow completed")
        return results
    
    async def analyze_stored_procedure(self, sql_code: str, business_context: str = "") -> Dict[str, str]:
        """
        Analyze a stored procedure using multiple agents
        
        Args:
            sql_code: The SQL code to analyze
            business_context: Business context about the SQL code
            
        Returns:
            Analysis results from multiple agents
        """
        results = {}
        
        # Business Analyst perspective
        business_analysis = await self.agents["business_analyst"].analyze_sql_procedure(
            sql_code=sql_code,
            business_context=business_context
        )
        results["business_analysis"] = business_analysis.response
        
        # Domain Expert perspective
        domain_analysis = await self.agents["domain_expert"].analyze_sql_details(sql_code)
        results["domain_analysis"] = domain_analysis.response
        
        # Azure Expert perspective on migration
        migration_considerations = await self.agents["azure_expert"].translate_sql_pattern_to_pyspark(
            sql_pattern=sql_code,
            context=business_context
        )
        results["migration_considerations"] = migration_considerations.response
        
        return results 