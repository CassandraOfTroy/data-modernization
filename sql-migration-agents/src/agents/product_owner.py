from src.core.base_agent import BaseAgent, AgentResponse
from typing import Dict, List

class ProductOwnerAgent(BaseAgent):
    """
    Product Owner Agent: Creates prioritized project backlog tasks based on analyses.
    """
    
    def __init__(self):
        system_prompt = """
        You are an experienced Product Owner specializing in data migration projects.
        Your role is to:
        
        1. Create well-defined user stories and tasks based on technical analysis
        2. Prioritize migration work to deliver business value early
        3. Define clear acceptance criteria for migration tasks
        4. Balance technical requirements with business priorities
        5. Create a structured migration roadmap
        6. Identify dependencies between tasks
        
        Provide clear, actionable outputs that help the team understand what needs to be built.
        Your focus is on structuring the work to maximize business value while managing technical complexity.
        """
        
        super().__init__(
            name="Product Owner",
            description="Creates and prioritizes project backlog for migration",
            system_prompt=system_prompt
        )
    
    async def create_user_stories(self, business_requirements: str, technical_analysis: str) -> AgentResponse:
        """
        Create user stories based on business requirements and technical analysis
        
        Args:
            business_requirements: Business requirements for the migration
            technical_analysis: Technical analysis of the system to migrate
            
        Returns:
            User stories for the migration
        """
        prompt = f"""
        Based on the following business requirements and technical analysis, 
        please create well-structured user stories for the SQL migration project:
        
        BUSINESS REQUIREMENTS:
        {business_requirements}
        
        TECHNICAL ANALYSIS:
        {technical_analysis}
        
        For each major feature or component of the migration, provide:
        1. A user story in the format "As a [user role], I want [feature], so that [benefit]"
        2. Acceptance criteria (3-5 specific, testable criteria)
        3. Technical notes relevant to implementation
        4. Relative priority (High, Medium, Low)
        5. Estimated complexity (High, Medium, Low)
        """
        
        return await self.process(prompt)
    
    async def create_sprint_plan(self, user_stories: List[Dict], team_capacity: int = 20) -> AgentResponse:
        """
        Create a sprint plan based on user stories and team capacity
        
        Args:
            user_stories: List of user stories with priorities and estimates
            team_capacity: Team capacity in story points
            
        Returns:
            Sprint plan with assigned stories
        """
        prompt = f"""
        Based on the following user stories and a team capacity of {team_capacity} story points,
        please create a sprint plan for the SQL migration project:
        
        USER STORIES:
        {user_stories}
        
        Please provide:
        1. Stories selected for the sprint with justification
        2. Total story points planned
        3. Sprint goal
        4. Dependencies and risks
        5. Any stories that were considered but deferred, with reasons
        """
        
        return await self.process(prompt)
    
    async def create_migration_roadmap(self, user_stories: List[Dict], timeline_constraints: str) -> AgentResponse:
        """
        Create a migration roadmap based on user stories and timeline constraints
        
        Args:
            user_stories: List of user stories to include in the roadmap
            timeline_constraints: Constraints on the migration timeline
            
        Returns:
            Migration roadmap
        """
        prompt = f"""
        Based on the following user stories and timeline constraints,
        please create a comprehensive migration roadmap:
        
        USER STORIES:
        {user_stories}
        
        TIMELINE CONSTRAINTS:
        {timeline_constraints}
        
        Please provide:
        1. A phased roadmap with specific milestones
        2. Key deliverables for each phase
        3. Dependencies between phases
        4. Risk mitigation strategies
        5. Business value delivery points
        6. Critical path items that need special attention
        """
        
        return await self.process(prompt)
    
    async def define_acceptance_criteria(self, feature_description: str, technical_considerations: str = "") -> AgentResponse:
        """
        Define acceptance criteria for a specific feature
        
        Args:
            feature_description: Description of the feature
            technical_considerations: Technical considerations for the feature
            
        Returns:
            Detailed acceptance criteria
        """
        prompt = f"""
        Please define detailed acceptance criteria for this feature in the SQL migration project:
        
        FEATURE DESCRIPTION:
        {feature_description}
        
        TECHNICAL CONSIDERATIONS:
        {technical_considerations}
        
        Please provide:
        1. Functional acceptance criteria (5-8 specific, testable criteria)
        2. Performance acceptance criteria
        3. Data quality acceptance criteria
        4. Integration acceptance criteria
        5. Edge cases that should be handled
        6. How success will be measured for this feature
        """
        
        return await self.process(prompt) 