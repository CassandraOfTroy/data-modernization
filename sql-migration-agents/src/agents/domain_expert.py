from src.core.base_agent import BaseAgent, AgentResponse

class DomainExpertAgent(BaseAgent):
    """
    Domain Expert (SQL Data Engineer) Agent: Provides deep technical and business knowledge 
    about the source code and data structures.
    """
    
    def __init__(self):
        system_prompt = """
        You are an experienced SQL Data Engineer with deep domain expertise in data warehousing and analytics. Your role is to:
        
        1. Analyze SQL code to identify its purpose, structure, and business rules
        2. Identify complex SQL patterns and their business implications
        3. Explain data flows, dependencies, and transformations
        4. Highlight potential issues in migration like performance bottlenecks or edge cases
        5. Provide technical context about SQL Server-specific features being used
        6. Explain the business domain context that the SQL code is operating within
        
        Provide detailed, technical explanations that demonstrate deep understanding of both SQL and 
        the business domain. Be specific about table structures, joins, data types, and business rules.
        """
        
        super().__init__(
            name="Domain Expert",
            description="SQL Data Engineer with deep domain expertise",
            system_prompt=system_prompt
        )
    
    async def analyze_sql_details(self, sql_code: str) -> AgentResponse:
        """
        Provide detailed analysis of SQL code, including business rules and technical details
        
        Args:
            sql_code: The SQL code to analyze
            
        Returns:
            Detailed analysis of the SQL code
        """
        prompt = f"""
        Please analyze this SQL code in detail and provide your expertise on:
        
        1. The exact business purpose of this code
        2. The data entities and their relationships
        3. The transformations and calculations being performed
        4. Any SQL Server-specific features that might be challenging to migrate
        5. The business rules and logic embedded in the code
        6. Performance considerations for the current implementation
        
        SQL CODE:
        ```sql
        {sql_code}
        ```
        """
        
        return await self.process(prompt)
    
    async def explain_complex_pattern(self, sql_pattern: str) -> AgentResponse:
        """
        Explain a complex SQL pattern in terms of its business purpose
        
        Args:
            sql_pattern: A complex SQL pattern to explain
            
        Returns:
            Explanation of the pattern's business purpose
        """
        prompt = f"""
        Please explain this complex SQL pattern in detail:
        
        ```sql
        {sql_pattern}
        ```
        
        Provide:
        1. The business purpose this pattern is trying to achieve
        2. The data transformation it's performing
        3. Why this specific SQL approach might have been chosen
        4. Any potential performance implications
        5. How this pattern could be translated to a more modern approach
        """
        
        return await self.process(prompt)
    
    async def identify_data_dependencies(self, sql_code: str, schema_info: str = "") -> AgentResponse:
        """
        Identify data dependencies in SQL code
        
        Args:
            sql_code: The SQL code to analyze
            schema_info: Additional information about the database schema
            
        Returns:
            Analysis of data dependencies
        """
        prompt = f"""
        Please analyze this SQL code and identify all data dependencies:
        
        ```sql
        {sql_code}
        ```
        
        SCHEMA INFORMATION:
        {schema_info}
        
        Please provide:
        1. A list of all source tables/views and their purpose in this procedure
        2. A list of all target tables/views being modified
        3. The relationships and data flow between these objects
        4. Any potential circular dependencies or ordering issues
        5. Critical business rules enforced through these dependencies
        """
        
        return await self.process(prompt) 