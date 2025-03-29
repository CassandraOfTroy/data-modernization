"""
Task templates for SQL Migration Agents.
Contains the templates for different tasks that can be sent to the agent group.
"""

def get_analysis_task_template() -> str:
    """
    Get the template for SQL stored procedure analysis task.
    
    Returns:
        Task template string with placeholders for SQL code and context
    """
    return """
    Please analyze this SQL stored procedure:
    
    ```sql
    {sql_code}
    ```
    
    Additional context: {context}
    
    I need:
    1. A clear explanation of the business purpose
    2. Technical analysis of SQL patterns used
    3. Data sources and their relationships
    4. Key business logic and transformations
    5. Potential challenges for migration to PySpark
    
    BusinessAnalyst - focus on business purpose and logic.
    DomainExpert - focus on SQL patterns and technical details.
    AzureExpert - focus on migration considerations for Microsoft Fabric.
    """

def get_migration_task_template() -> str:
    """
    Get the template for SQL stored procedure migration task.
    
    Returns:
        Task template string with placeholders for SQL code and context
    """
    return """
    Please migrate this SQL stored procedure to PySpark for Microsoft Fabric:
    
    ```sql
    {sql_code}
    ```
    
    Additional context: {context}
    
    I need a complete migration plan and implementation following the medallion architecture with:
    1. Business analysis and migration plan
    2. PySpark code for bronze, silver, and gold layers
    3. Implementation guidelines and best practices
    4. Test cases to validate the migration
    
    The final PySpark code should be production-ready, well-commented, and follow Microsoft Fabric best practices.
    
    BusinessAnalyst - analyze business requirements
    ProductOwner - create the migration plan
    AzureDataEngineer - translate SQL to PySpark with medallion architecture
    TechLead - review code quality and architecture
    TestingAgent - create test cases
    """ 