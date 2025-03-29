# Integrating SQL Migration Agents with Azure AI Foundry

This document provides guidance on integrating the SQL Migration Agents with Azure AI Foundry.

## Overview

Azure AI Foundry provides a platform for building, deploying, and managing AI applications. The SQL Migration Agents can be integrated with AI Foundry to leverage its orchestration capabilities, knowledge management, and deployment infrastructure.

## Integration Approaches

### 1. Package Agents as AI Foundry Skills

Each agent in the system can be packaged as a separate AI Foundry skill:

1. **Create a Skill for Each Agent**:
   - Business Analyst Skill
   - Domain Expert Skill
   - Azure Cloud Expert Skill
   - Product Owner Skill
   - Azure Data Engineer Skill
   - Tech Lead Skill
   - Testing Agent Skill

2. **Define Skill Interfaces**:
   Each skill should define its input and output schema based on the agent's capabilities.

3. **Implement Custom Skill Logic**:
   Use the Azure AI Foundry SDK to wrap each agent's core functionality as a skill.

Example of packaging the Business Analyst agent as a skill:

```python
from azure.ai.foundry.skills import Skill, SkillInput, SkillOutput

class BusinessAnalystSkill(Skill):
    def __init__(self):
        super().__init__(
            name="business_analyst",
            description="Analyzes business requirements and coordinates with technical experts"
        )
        # Initialize the agent here
        self.agent = BusinessAnalystAgent()
    
    async def execute(self, inputs: SkillInput) -> SkillOutput:
        # Extract inputs
        sql_code = inputs.get("sql_code")
        business_context = inputs.get("business_context", "")
        
        # Call the agent
        response = await self.agent.analyze_sql_procedure(sql_code, business_context)
        
        # Return the output
        return SkillOutput(
            analysis=response.response,
            success=response.success
        )
```

### 2. Create Multi-Agent Workflows in AI Foundry

Use AI Foundry's workflow capabilities to orchestrate the interactions between agents:

1. **Define a Workflow**:
   Create a workflow that coordinates the entire migration process, from analysis to testing.

2. **Connect Agent Skills**:
   Connect the agent skills in the workflow, defining how data flows between them.

3. **Add Decision Points**:
   Add decision points where human approval or feedback is required.

Example workflow definition:

```yaml
name: SQLMigrationWorkflow
description: Migrates SQL stored procedures to PySpark
steps:
  - name: AnalyzeSQL
    skill: business_analyst
    inputs:
      sql_code: ${workflow.inputs.sql_code}
      business_context: ${workflow.inputs.business_context}
    outputs:
      business_analysis: ${business_analyst.outputs.analysis}
  
  - name: TechnicalAnalysis
    skill: domain_expert
    inputs:
      sql_code: ${workflow.inputs.sql_code}
    outputs:
      technical_analysis: ${domain_expert.outputs.analysis}
  
  # Additional steps...
```

### 3. Use AI Foundry Knowledge Bases

Store domain knowledge, best practices, and examples in AI Foundry knowledge bases:

1. **Create Knowledge Bases**:
   - SQL Server Best Practices KB
   - PySpark Patterns KB
   - Microsoft Fabric Architecture KB
   - Data Migration Patterns KB

2. **Connect Knowledge Bases to Skills**:
   Allow agents to query knowledge bases to enhance their responses.

### 4. Deploy as an AI Foundry Application

Package the entire solution as an AI Foundry application:

1. **Create an Application Definition**:
   Define the application, its components, and how they interact.

2. **Deploy to Azure**:
   Deploy the application to your Azure environment through AI Foundry.

3. **Configure Authentication and Authorization**:
   Set up authentication and authorization for your application.

## Step-by-Step Integration Guide

1. **Set Up Azure AI Foundry**:
   - Create an Azure AI Foundry resource
   - Set up a project for SQL Migration

2. **Package Agents as Skills**:
   - For each agent, create a skill class
   - Define input/output schemas
   - Implement the execution logic

3. **Define Knowledge Bases**:
   - Create knowledge bases with relevant domain information
   - Index example SQL procedures and PySpark translations

4. **Create Workflow**:
   - Define the migration workflow
   - Connect skills in the proper sequence
   - Add human-in-the-loop approval steps where needed

5. **Configure Deployment**:
   - Set up compute resources
   - Configure scaling settings
   - Set up monitoring

6. **Test and Validate**:
   - Test the workflow with sample procedures
   - Validate outputs at each step
   - Tune agent prompts as needed

## Example Usage

Once deployed to AI Foundry, you can use the application through:

1. **AI Foundry Portal**:
   - Upload SQL files through the portal
   - Monitor workflow execution
   - Review and approve outputs

2. **API Integration**:
   - Call the workflow API from your applications
   - Use webhooks for asynchronous processing

3. **Batch Processing**:
   - Process multiple SQL procedures in batch
   - Schedule regular migration jobs

## Best Practices

1. **Prompt Engineering**:
   - Tune agent prompts in AI Foundry for best results
   - Use prompt versioning to track changes

2. **Performance Optimization**:
   - Configure caching for repeated operations
   - Set appropriate timeout values

3. **Error Handling**:
   - Implement comprehensive error handling
   - Add retry logic for transient failures

4. **Authentication**:
   - Use managed identities for secure access to Azure resources
   - Implement proper access control

5. **Monitoring**:
   - Set up logging and monitoring
   - Configure alerts for workflow failures 