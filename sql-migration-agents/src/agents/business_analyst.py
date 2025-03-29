from src.core.base_agent import BaseAgent, AgentResponse
from typing import Dict

class BusinessAnalystAgent(BaseAgent):
    """
    Business Analyst Agent: Coordinates the analysis by interfacing with domain experts,
    gathering requirements, and translating technical findings into business context.
    """
    
    def __init__(self):
        system_prompt = """
        You are a skilled Business Analyst specializing in data migration projects. Your role is to:
        
        1. Gather and analyze business and technical requirements related to SQL data procedures
        2. Coordinate with domain experts and engineers to understand data flows and dependencies
        3. Translate technical findings into business context
        4. Create clear documentation for migration projects
        5. Identify business risks and impact of the migration
        6. Ensure the migrated solution meets business requirements
        
        Your focus is on bridging the gap between technical details and business needs.
        Provide clear explanations that both technical and non-technical stakeholders can understand.
        """
        
        super().__init__(
            name="Business Analyst",
            description="Analyzes business requirements and coordinates with technical experts",
            system_prompt=system_prompt
        )
    
    async def analyze_sql_procedure(self, sql_code: str, business_context: str = "") -> AgentResponse:
        """
        Analyze a SQL stored procedure to understand its business purpose and data flows
        
        Args:
            sql_code: The SQL code to analyze
            business_context: Additional business context about the procedure
            
        Returns:
            Analysis of the business purpose and data flows
        """
        prompt = f"""
        Please analyze this SQL stored procedure and help me understand:
        
        1. What is the business purpose of this procedure?
        2. What data entities does it work with?
        3. What are the key business rules implemented?
        4. What are potential risks or special considerations for migration?
        
        SQL CODE:
        ```sql
        {sql_code}
        ```
        
        BUSINESS CONTEXT:
        {business_context}
        """
        
        return await self.process(prompt)
    
    async def create_business_requirements(self, analysis_results: Dict) -> AgentResponse:
        """
        Create business requirements based on analysis results
        
        Args:
            analysis_results: Results from SQL analysis
            
        Returns:
            Business requirements document
        """
        prompt = f"""
        Based on the analysis of the SQL procedure, please create a clear business requirements document that includes:
        
        1. Business objectives of the migration
        2. Critical functionality that must be preserved
        3. Business rules that must be implemented
        4. Data quality expectations
        5. Performance requirements
        6. Success criteria for the migration
        
        Analysis results:
        {analysis_results}
        """
        
        return await self.process(prompt)
    
    async def validate_technical_solution(self, business_requirements: str, technical_solution: str) -> AgentResponse:
        """
        Validate that a technical solution meets business requirements
        
        Args:
            business_requirements: The business requirements
            technical_solution: The proposed technical solution
            
        Returns:
            Assessment of whether the solution meets requirements
        """
        prompt = f"""
        Please review the proposed technical solution and assess whether it meets all the business requirements:
        
        BUSINESS REQUIREMENTS:
        {business_requirements}
        
        TECHNICAL SOLUTION:
        {technical_solution}
        
        Please provide:
        1. An assessment of whether each requirement is met
        2. Any concerns or risks to address
        3. Questions that need clarification
        4. Suggestions for improvement from a business perspective
        """
        
        return await self.process(prompt) 