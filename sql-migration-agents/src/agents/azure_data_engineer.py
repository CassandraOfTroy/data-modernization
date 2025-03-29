from src.core.base_agent import BaseAgent, AgentResponse

class AzureDataEngineerAgent(BaseAgent):
    """
    Azure Data Engineer Agent: Translates SQL to PySpark code, focusing on creating 
    layered RFM projects with equivalent functionality.
    """
    
    def __init__(self):
        system_prompt = """
        You are an expert Azure Data Engineer specialized in translating SQL Server stored procedures 
        to PySpark code running in Microsoft Fabric. Your expertise includes:
        
        1. Writing high-quality, production-ready PySpark code
        2. Implementing medallion architecture (bronze, silver, gold layers)
        3. Optimizing PySpark for performance at scale
        4. Understanding SQL Server-specific features and their PySpark equivalents
        5. Implementing proper error handling and logging in PySpark
        6. Creating modular, maintainable code structures
        7. Implementing RFM (Recency, Frequency, Monetary) analysis
        
        When translating code, focus on:
        - Maintaining functional equivalence with the original SQL
        - Following PySpark best practices
        - Creating a proper layered architecture
        - Providing comprehensive comments
        - Ensuring code is optimized for Microsoft Fabric
        """
        
        super().__init__(
            name="Azure Data Engineer",
            description="Translates SQL to PySpark with a focus on medallion architecture",
            system_prompt=system_prompt
        )
    
    async def translate_full_procedure(self, sql_procedure: str, additional_context: str = "") -> AgentResponse:
        """
        Translate a full SQL stored procedure to PySpark
        
        Args:
            sql_procedure: The SQL stored procedure to translate
            additional_context: Additional context about the procedure
            
        Returns:
            Complete PySpark translation
        """
        prompt = f"""
        Please translate this SQL Server stored procedure to efficient PySpark code for Microsoft Fabric:
        
        ```sql
        {sql_procedure}
        ```
        
        ADDITIONAL CONTEXT:
        {additional_context}
        
        Please provide:
        1. Complete PySpark code implementation
        2. A medallion architecture approach (bronze, silver, gold layers)
        3. Clear module structure and organization
        4. Comprehensive error handling and logging
        5. Performance optimizations for Microsoft Fabric
        
        The code should be production-ready, well-commented, and follow best practices.
        Use only features compatible with Microsoft Fabric.
        """
        
        return await self.process(prompt)
    
    async def create_bronze_layer(self, sql_procedure: str, source_tables: str) -> AgentResponse:
        """
        Create the bronze layer implementation for a SQL procedure
        
        Args:
            sql_procedure: The SQL stored procedure to reference
            source_tables: Information about source tables
            
        Returns:
            Bronze layer implementation
        """
        prompt = f"""
        Based on this SQL procedure and source table information, create a bronze layer implementation
        in PySpark that extracts the raw data needed for the procedure:
        
        SQL PROCEDURE:
        ```sql
        {sql_procedure}
        ```
        
        SOURCE TABLES:
        {source_tables}
        
        Please provide:
        1. PySpark code to extract data from source systems
        2. Schema definitions for the bronze layer tables
        3. Incremental loading strategy if appropriate
        4. Data validation at the bronze layer
        5. Considerations for handling source data quality issues
        6. Appropriate partitioning strategy
        
        The code should be optimized for Microsoft Fabric and follow bronze layer best practices.
        """
        
        return await self.process(prompt)
    
    async def create_silver_layer(self, bronze_implementation: str, business_rules: str) -> AgentResponse:
        """
        Create the silver layer implementation based on bronze layer and business rules
        
        Args:
            bronze_implementation: Bronze layer implementation details
            business_rules: Business rules to implement in the silver layer
            
        Returns:
            Silver layer implementation
        """
        prompt = f"""
        Based on this bronze layer implementation and business rules, create a silver layer
        implementation in PySpark:
        
        BRONZE IMPLEMENTATION:
        {bronze_implementation}
        
        BUSINESS RULES:
        {business_rules}
        
        Please provide:
        1. PySpark code for the silver layer transformations
        2. Data quality checks and cleansing logic
        3. Schema standardization and enrichment
        4. Implementation of core business rules
        5. Proper handling of schema evolution
        6. Appropriate join strategies and optimizations
        
        The code should follow silver layer best practices and be optimized for Microsoft Fabric.
        """
        
        return await self.process(prompt)
    
    async def create_gold_layer(self, silver_implementation: str, analytics_requirements: str) -> AgentResponse:
        """
        Create the gold layer implementation based on silver layer and analytics requirements
        
        Args:
            silver_implementation: Silver layer implementation details
            analytics_requirements: Requirements for analytics outputs
            
        Returns:
            Gold layer implementation
        """
        prompt = f"""
        Based on this silver layer implementation and analytics requirements, create a gold layer
        implementation in PySpark:
        
        SILVER IMPLEMENTATION:
        {silver_implementation}
        
        ANALYTICS REQUIREMENTS:
        {analytics_requirements}
        
        Please provide:
        1. PySpark code for the gold layer aggregations and models
        2. Implementation of complex business metrics
        3. Optimization for query performance
        4. Documentation of business metrics calculated
        5. Implementation of dimensional models if appropriate
        6. RFM analysis implementation if applicable
        
        The code should follow gold layer best practices and be optimized for Microsoft Fabric.
        Focus on creating business-ready, well-documented datasets.
        """
        
        return await self.process(prompt) 