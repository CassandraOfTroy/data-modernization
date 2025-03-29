from src.core.base_agent import BaseAgent, AgentResponse

class TechLeadAgent(BaseAgent):
    """
    Tech Lead Agent: Reviews, refactors, and improves code quality, 
    ensuring best practices and maintainability.
    """
    
    def __init__(self):
        system_prompt = """
        You are a senior Tech Lead with extensive experience in data engineering and cloud architecture.
        Your role is to:
        
        1. Review code for quality, performance, and maintainability
        2. Refactor code to follow best practices
        3. Identify and address technical debt
        4. Ensure architecture follows best practices
        5. Provide constructive feedback to improve code
        6. Standardize code patterns and practices
        
        Your feedback should be specific, actionable, and focused on making the code production-ready.
        Focus on cloud best practices, code organization, error handling, logging, and performance optimizations.
        Your reviews should balance technical excellence with pragmatism.
        """
        
        super().__init__(
            name="Tech Lead",
            description="Reviews and improves code quality and architecture",
            system_prompt=system_prompt
        )
    
    async def review_code(self, code: str, context: str = "") -> AgentResponse:
        """
        Review code for quality, performance, and maintainability
        
        Args:
            code: The code to review
            context: Additional context about the code
            
        Returns:
            Code review with feedback
        """
        prompt = f"""
        Please review this code and provide detailed feedback:
        
        ```python
        {code}
        ```
        
        CONTEXT:
        {context}
        
        Please provide:
        1. Overall assessment of code quality
        2. Specific issues that need to be addressed (with line references)
        3. Improvements for readability and maintainability
        4. Performance optimization suggestions
        5. Error handling and logging improvements
        6. Architecture and design pattern recommendations
        
        Your review should be constructive and actionable, focusing on making this code production-ready.
        """
        
        return await self.process(prompt)
    
    async def refactor_code(self, code: str, review_feedback: str = "") -> AgentResponse:
        """
        Refactor code based on review feedback
        
        Args:
            code: The code to refactor
            review_feedback: Feedback from code review
            
        Returns:
            Refactored code
        """
        prompt = f"""
        Please refactor this code based on the provided feedback:
        
        ORIGINAL CODE:
        ```python
        {code}
        ```
        
        REVIEW FEEDBACK:
        {review_feedback}
        
        Please provide:
        1. The refactored code with improvements implemented
        2. Explanation of major changes made
        3. How the refactoring addresses the feedback
        4. Any additional improvements you've made beyond the feedback
        
        The refactored code should follow best practices for PySpark in Microsoft Fabric and be production-ready.
        """
        
        return await self.process(prompt)
    
    async def review_architecture(self, architecture_description: str, requirements: str) -> AgentResponse:
        """
        Review a proposed architecture
        
        Args:
            architecture_description: Description of the proposed architecture
            requirements: Requirements the architecture should meet
            
        Returns:
            Architecture review with recommendations
        """
        prompt = f"""
        Please review this proposed architecture and provide expert feedback:
        
        ARCHITECTURE DESCRIPTION:
        {architecture_description}
        
        REQUIREMENTS:
        {requirements}
        
        Please provide:
        1. Overall assessment of the architecture
        2. Strengths of the proposed design
        3. Weaknesses or risks in the architecture
        4. Specific recommendations for improvement
        5. Alternative approaches to consider
        6. Cloud architecture best practices that should be applied
        
        Your review should focus on ensuring the architecture is robust, scalable, secure, 
        and follows Azure cloud best practices.
        """
        
        return await self.process(prompt)
    
    async def provide_coding_standards(self, language: str = "PySpark", context: str = "") -> AgentResponse:
        """
        Provide coding standards and best practices
        
        Args:
            language: The programming language or framework
            context: Additional context about the project
            
        Returns:
            Coding standards and best practices
        """
        prompt = f"""
        Please provide detailed coding standards and best practices for {language} 
        in the context of data migration projects in Microsoft Fabric:
        
        PROJECT CONTEXT:
        {context}
        
        Please include:
        1. Code organization and structure
        2. Naming conventions
        3. Documentation standards
        4. Error handling patterns
        5. Logging best practices
        6. Performance optimization guidelines
        7. Testing approaches
        8. Specific Microsoft Fabric considerations
        
        The standards should be concrete, actionable, and focused on production-quality code.
        """
        
        return await self.process(prompt) 