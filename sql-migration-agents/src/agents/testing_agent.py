from src.core.base_agent import BaseAgent, AgentResponse

class TestingAgent(BaseAgent):
    """
    Testing Agent: Creates and executes test cases to validate that migrated code 
    produces identical results.
    """
    
    def __init__(self):
        system_prompt = """
        You are an expert Testing Engineer specializing in data migration validation. Your role is to:
        
        1. Create comprehensive test cases for SQL to PySpark migrations
        2. Design data validation strategies to verify data equivalence
        3. Develop test harnesses to compare original and migrated code results
        4. Identify edge cases that should be tested
        5. Create performance tests to validate non-functional requirements
        6. Document testing approaches and results
        
        Your test plans should be thorough and focus on validating both functional correctness 
        and performance characteristics. Ensure that tests cover a wide range of scenarios and edge cases.
        Focus on automated testing approaches that can be integrated into CI/CD pipelines.
        """
        
        super().__init__(
            name="Testing Agent",
            description="Creates test cases to validate migrated code",
            system_prompt=system_prompt
        )
    
    async def create_test_plan(self, sql_code: str, pyspark_code: str, requirements: str = "") -> AgentResponse:
        """
        Create a comprehensive test plan for validating a SQL to PySpark migration
        
        Args:
            sql_code: The original SQL code
            pyspark_code: The migrated PySpark code
            requirements: Additional requirements for the migration
            
        Returns:
            Comprehensive test plan
        """
        prompt = f"""
        Please create a comprehensive test plan for validating this SQL to PySpark migration:
        
        ORIGINAL SQL CODE:
        ```sql
        {sql_code}
        ```
        
        MIGRATED PYSPARK CODE:
        ```python
        {pyspark_code}
        ```
        
        REQUIREMENTS:
        {requirements}
        
        Please provide:
        1. Test strategy overview
        2. Data validation test cases (at least 5-10)
        3. Functional validation test cases (at least 5-10)
        4. Edge case and error handling tests
        5. Performance test cases
        6. Test data generation approach
        7. Test environment requirements
        8. Recommended testing tools and frameworks
        
        The test plan should ensure full functional equivalence between the original SQL 
        and the new PySpark implementation.
        """
        
        return await self.process(prompt)
    
    async def generate_test_cases(self, feature_description: str, acceptance_criteria: str) -> AgentResponse:
        """
        Generate detailed test cases based on feature description and acceptance criteria
        
        Args:
            feature_description: Description of the feature to test
            acceptance_criteria: Acceptance criteria for the feature
            
        Returns:
            Detailed test cases
        """
        prompt = f"""
        Please generate detailed test cases for this feature:
        
        FEATURE DESCRIPTION:
        {feature_description}
        
        ACCEPTANCE CRITERIA:
        {acceptance_criteria}
        
        For each acceptance criterion, please provide:
        1. Test case ID and name
        2. Preconditions
        3. Test steps with expected results
        4. Pass/fail criteria
        5. Test data requirements
        6. Test environment requirements
        
        Include both happy path and negative/edge case scenarios. Focus on thorough validation
        of the feature against all acceptance criteria.
        """
        
        return await self.process(prompt)
    
    async def create_data_equivalence_test(self, sql_query: str, pyspark_code: str) -> AgentResponse:
        """
        Create a data equivalence test to validate that SQL and PySpark produce the same results
        
        Args:
            sql_query: The SQL query to validate
            pyspark_code: The PySpark code to validate
            
        Returns:
            Data equivalence test implementation
        """
        prompt = f"""
        Please create a data equivalence test that validates the PySpark code produces 
        the same results as the original SQL query:
        
        SQL QUERY:
        ```sql
        {sql_query}
        ```
        
        PYSPARK CODE:
        ```python
        {pyspark_code}
        ```
        
        Please provide:
        1. A Python test script that runs both the SQL query and PySpark code
        2. Logic to compare the results for equivalence
        3. Validation criteria (eg. row counts, column values, aggregates)
        4. Reporting of any differences found
        5. Test data setup requirements
        6. Approach to handle large datasets in testing
        
        The test should be automated and provide clear pass/fail results.
        """
        
        return await self.process(prompt)
    
    async def create_performance_test(self, pyspark_code: str, performance_requirements: str) -> AgentResponse:
        """
        Create a performance test for PySpark code
        
        Args:
            pyspark_code: The PySpark code to test
            performance_requirements: Performance requirements for the code
            
        Returns:
            Performance test implementation
        """
        prompt = f"""
        Please create a performance test script for this PySpark code based on the performance requirements:
        
        PYSPARK CODE:
        ```python
        {pyspark_code}
        ```
        
        PERFORMANCE REQUIREMENTS:
        {performance_requirements}
        
        Please provide:
        1. A Python test script that measures performance metrics
        2. Parameters to vary for performance testing (eg. data volume, partition count)
        3. Metrics to capture (eg. execution time, resource utilization)
        4. Visualization or reporting of results
        5. Pass/fail criteria based on requirements
        6. Recommendations for performance optimization
        
        The test should be able to run in different environments and with different data volumes.
        """
        
        return await self.process(prompt) 