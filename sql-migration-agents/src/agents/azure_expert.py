from src.core.base_agent import BaseAgent, AgentResponse

class AzureCloudExpertAgent(BaseAgent):
    """
    Azure Cloud Expert Agent: Contributes specialized knowledge about PySpark 
    and Azure services.
    """
    
    def __init__(self):
        system_prompt = """
        You are an Azure Cloud and PySpark expert with deep knowledge of data engineering on Azure.
        Your expertise covers:
        
        1. Microsoft Fabric and its capabilities
        2. PySpark programming and best practices
        3. Azure Data Factory for orchestration
        4. Azure Data Lake Storage Gen2
        5. Microsoft Fabric Spark compute and Lakehouse architecture
        6. Performance optimization in distributed compute environments
        7. RFM (Recency, Frequency, Monetary) analysis patterns in PySpark
        
        Provide detailed technical guidance on implementing solutions in Azure's data stack.
        Focus on Microsoft Fabric as the primary compute platform. Be specific about implementation patterns,
        performance considerations, and Azure-specific optimizations.
        """
        
        super().__init__(
            name="Azure Cloud Expert",
            description="Expert in Azure data services and PySpark",
            system_prompt=system_prompt
        )
    
    async def recommend_azure_architecture(self, requirements: str, data_volume: str = "medium") -> AgentResponse:
        """
        Recommend an Azure architecture based on requirements
        
        Args:
            requirements: The business and technical requirements
            data_volume: The expected data volume (small, medium, large)
            
        Returns:
            Recommended Azure architecture
        """
        prompt = f"""
        Based on the following requirements and a {data_volume} data volume, recommend 
        an Azure architecture for data processing:
        
        REQUIREMENTS:
        {requirements}
        
        Please provide:
        1. A recommended architecture using Microsoft Fabric as the core compute platform
        2. Specific Azure services and their roles in the solution
        3. How data will flow through the system
        4. Cost considerations and optimization recommendations
        5. Scalability aspects of the design
        6. Security considerations for the architecture
        """
        
        return await self.process(prompt)
    
    async def translate_sql_pattern_to_pyspark(self, sql_pattern: str, context: str = "") -> AgentResponse:
        """
        Translate a SQL pattern to PySpark
        
        Args:
            sql_pattern: The SQL pattern to translate
            context: Additional context about the pattern
            
        Returns:
            PySpark equivalent of the SQL pattern
        """
        prompt = f"""
        Please translate this SQL pattern to efficient PySpark code:
        
        ```sql
        {sql_pattern}
        ```
        
        CONTEXT:
        {context}
        
        Please provide:
        1. The equivalent PySpark code using DataFrame API
        2. Explanation of the translation approach
        3. Any optimization techniques applied
        4. How this would be executed in Microsoft Fabric
        5. Any considerations for large-scale data processing
        """
        
        return await self.process(prompt)
    
    async def optimize_pyspark_code(self, pyspark_code: str, performance_requirements: str = "") -> AgentResponse:
        """
        Optimize PySpark code for performance
        
        Args:
            pyspark_code: The PySpark code to optimize
            performance_requirements: Performance requirements or constraints
            
        Returns:
            Optimized PySpark code
        """
        prompt = f"""
        Please optimize this PySpark code for better performance in Microsoft Fabric:
        
        ```python
        {pyspark_code}
        ```
        
        PERFORMANCE REQUIREMENTS:
        {performance_requirements}
        
        Please provide:
        1. Optimized version of the code
        2. Specific optimizations applied and why
        3. Configuration settings that would improve performance
        4. How to monitor this code's performance in Microsoft Fabric
        5. Trade-offs made in the optimization process
        """
        
        return await self.process(prompt)
    
    async def design_medallion_architecture(self, data_entities: str, business_requirements: str) -> AgentResponse:
        """
        Design a medallion architecture (bronze, silver, gold) for data processing
        
        Args:
            data_entities: Description of data entities to process
            business_requirements: Business requirements for the solution
            
        Returns:
            Detailed medallion architecture design
        """
        prompt = f"""
        Please design a medallion architecture (bronze, silver, gold) for processing these data entities in Microsoft Fabric:
        
        DATA ENTITIES:
        {data_entities}
        
        BUSINESS REQUIREMENTS:
        {business_requirements}
        
        Please provide:
        1. Detailed structure for bronze layer (raw data)
            - Table designs
            - Ingestion patterns
            - Data formats and partitioning
        
        2. Detailed structure for silver layer (clean, conformed data)
            - Transformation logic
            - Data quality checks
            - Schema evolution handling
        
        3. Detailed structure for gold layer (business-ready data)
            - Business aggregations
            - Performance optimization
            - Query patterns
        
        4. Implementation approach using PySpark in Microsoft Fabric
        5. Data flow between layers
        6. Governance and monitoring approach
        """
        
        return await self.process(prompt) 