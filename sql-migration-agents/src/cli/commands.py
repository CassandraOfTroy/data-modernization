"""
Command handlers for SQL Migration Agents CLI.
"""

import os
from pathlib import Path
from loguru import logger
import sqlparse
from typing import Dict, Any
import json

from src.agents.agent_manager import AgentManager
from src.agents.tasks import get_analysis_task_template, get_migration_task_template
from src.utils.file_utils import read_sql_file, write_output_file
from src.agents.utils import extract_structured_code

def analyze_sql(args):
    """
    Analyze a SQL stored procedure.
    
    Args:
        args: Command-line arguments
    """
    try:
        # Read SQL file
        sql_code = read_sql_file(args.sql_file)
        
        # Format the SQL for better readability
        formatted_sql = sqlparse.format(sql_code, reindent=True, keyword_case='upper')
        
        # Initialize agent manager
        agent_manager = AgentManager()
        
        # Create the task message from template
        task_message = get_analysis_task_template().format(
            sql_code=formatted_sql,
            context=args.context
        )
        
        # Execute the task and capture the results
        result_data = agent_manager.execute_task(task_message)
        
        # Check if execution was successful
        if not result_data.get('success', False):
            error_msg = result_data.get('error', 'Unknown error')
            logger.error(f"Task execution failed: {error_msg}")
            print(f"Error: Task execution failed - {error_msg}")
            return 1
        
        # Extract the conversation messages from the returned data
        conversation_messages = result_data.get('full_conversation', [])
        
        # Check if any agents are missing
        missing_agents = result_data.get('missing_agents', [])
        if missing_agents:
            logger.warning(f"Missing responses from agents: {missing_agents}")
            print(f"Warning: Some agents did not respond: {', '.join(missing_agents)}")
        
        # Get the results by passing the captured messages
        results = agent_manager.get_results_for_analysis(conversation_messages)
        
        # Check if results were successfully extracted
        if not conversation_messages:
            logger.warning("Conversation generated no messages.")
            print("Warning: The agent conversation generated no messages.")
            return 0

        # Ensure keys exist before accessing, provide defaults
        business_analysis = results.get("business_analysis", "")
        technical_analysis = results.get("technical_analysis", "")
        azure_recommendations = results.get("azure_recommendations", "")
        
        # Print summary
        print("\n=== SQL Analysis Summary ===")
        print(f"\nFile: {args.sql_file}")
        print(f"Context: {args.context or 'None provided'}")
        print(f"Agents responded: {len(result_data.get('agents_responded', []))}")
        
        print("\nBusiness Analysis:")
        print("-" * 40)
        print(business_analysis[:500] + "..." if len(business_analysis) > 500 else business_analysis)
        
        print("\nTechnical Analysis:")
        print("-" * 40)
        print(technical_analysis[:500] + "..." if len(technical_analysis) > 500 else technical_analysis)
        
        print("\nAzure Recommendations:")
        print("-" * 40)
        print(azure_recommendations[:500] + "..." if len(azure_recommendations) > 500 else azure_recommendations)
        
        # Save results to files
        output_dir = args.output_dir or "data/output/analysis"
        os.makedirs(output_dir, exist_ok=True)
        
        # Save individual analysis files
        write_output_file(os.path.join(output_dir, "business_analysis.md"), 
                         f"# Business Analysis\n\n{business_analysis}")
        write_output_file(os.path.join(output_dir, "technical_analysis.md"), 
                         f"# Technical Analysis\n\n{technical_analysis}")
        write_output_file(os.path.join(output_dir, "azure_recommendations.md"), 
                         f"# Azure Recommendations\n\n{azure_recommendations}")
        
        # Save combined analysis
        combined_analysis = f"""# SQL Analysis Report

## File Information
- **SQL File**: {args.sql_file}
- **Context**: {args.context or 'None provided'}
- **Analysis Date**: {os.environ.get('TZ', 'UTC')}

## Business Analysis
{business_analysis}

## Technical Analysis
{technical_analysis}

## Azure Recommendations
{azure_recommendations}
"""
        write_output_file(os.path.join(output_dir, "analysis_report.md"), combined_analysis)
        
        # Save full conversation for reference
        write_output_file(os.path.join(output_dir, "full_conversation.json"), 
                         conversation_messages, is_json=True)
        
        logger.info(f"Analysis complete. Results saved to {output_dir}")
        print(f"\nFull analysis results saved to {output_dir}")
        
    except FileNotFoundError as e:
        logger.error(f"SQL file not found: {str(e)}")
        print(f"Error: {str(e)}")
        return 1
    except Exception as e:
        logger.error(f"Error analyzing SQL: {str(e)}", exc_info=True)
        print(f"Error: {str(e)}")
        return 1
    
    return 0

def migrate_sql(args):
    """
    Migrate a SQL stored procedure to PySpark.
    
    Args:
        args: Command-line arguments
    """
    try:
        output_dir = args.output_dir or f"data/output/{Path(args.sql_file).stem}"
        
        # Read SQL file
        sql_code = read_sql_file(args.sql_file)
        
        # Format the SQL for better readability
        formatted_sql = sqlparse.format(sql_code, reindent=True, keyword_case='upper')
        
        # Initialize agent manager
        agent_manager = AgentManager()
        
        # Create the task message from template
        task_message = get_migration_task_template().format(
            sql_code=formatted_sql,
            context=args.context
        )
        
        logger.info(f"Starting migration for {args.sql_file}")
        
        # Execute the task and capture the results
        result_data = agent_manager.execute_task(task_message)
        
        # Check if execution was successful
        if not result_data.get('success', False):
            error_msg = result_data.get('error', 'Unknown error')
            logger.error(f"Task execution failed: {error_msg}")
            print(f"Error: Task execution failed - {error_msg}")
            return 1
        
        # Extract the conversation messages from the returned data
        conversation_messages = result_data.get('full_conversation', [])
        
        # Check if any agents are missing
        missing_agents = result_data.get('missing_agents', [])
        if missing_agents:
            logger.warning(f"Missing responses from agents: {missing_agents}")
            print(f"Warning: Some agents did not respond: {', '.join(missing_agents)}")
        
        # Get the results by passing the captured messages
        results = agent_manager.get_results_for_migration(conversation_messages)
        
        # Check if results were successfully extracted
        if not conversation_messages:
            logger.warning("Conversation generated no messages.")
            print("Warning: The agent conversation generated no messages.")
            return 1
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Extract structured code from the results
        pyspark_code = results.get("pyspark_code", [])
        test_cases = results.get("test_cases", [])
        migration_plan = results.get("migration_plan", "")
        
        # Try to extract structured code from AzureDataEngineer's response
        from src.agents.utils import extract_response
        azure_response = extract_response(conversation_messages, "AzureDataEngineer")
        structured_code = extract_structured_code(azure_response)
        
        # Save structured code files
        files_saved = []
        
        # Save Bronze Layer
        if structured_code["bronze"]:
            bronze_path = os.path.join(output_dir, "bronze_layer.py")
            write_output_file(bronze_path, structured_code["bronze"])
            files_saved.append("bronze_layer.py")
            logger.info(f"Saved bronze layer to: {bronze_path}")
        
        # Save Stage 1 Base Data
        if structured_code["stage1"]:
            stage1_path = os.path.join(output_dir, "stage1_base_data.py")
            write_output_file(stage1_path, structured_code["stage1"])
            files_saved.append("stage1_base_data.py")
            logger.info(f"Saved Stage 1 Base Data to: {stage1_path}")
        
        # Save Stage 2 Advanced Analytics
        if structured_code["stage2"]:
            stage2_path = os.path.join(output_dir, "stage2_advanced_analytics.py")
            write_output_file(stage2_path, structured_code["stage2"])
            files_saved.append("stage2_advanced_analytics.py")
            logger.info(f"Saved Stage 2 Advanced Analytics to: {stage2_path}")
        
        # Save Silver Layer (if exists separately)
        if structured_code["silver"]:
            silver_path = os.path.join(output_dir, "silver_layer.py")
            write_output_file(silver_path, structured_code["silver"])
            files_saved.append("silver_layer.py")
            logger.info(f"Saved silver layer to: {silver_path}")
        
        # Save Gold Layer
        if structured_code["gold"]:
            gold_path = os.path.join(output_dir, "gold_layer.py")
            write_output_file(gold_path, structured_code["gold"])
            files_saved.append("gold_layer.py")
            logger.info(f"Saved gold layer to: {gold_path}")
        
        # If no structured code was found, save all code blocks
        if not any(structured_code.values()) and pyspark_code:
            logger.warning("No structured code found, saving all code blocks")
            for i, code_block in enumerate(pyspark_code):
                code_path = os.path.join(output_dir, f"pyspark_code_{i+1}.py")
                write_output_file(code_path, code_block)
                files_saved.append(f"pyspark_code_{i+1}.py")
        
        # Save test cases
        if test_cases:
            test_path = os.path.join(output_dir, "test_migration.py")
            # Combine all test cases into one file
            combined_tests = "# Migration Test Cases\n\n" + "\n\n".join(test_cases)
            write_output_file(test_path, combined_tests)
            files_saved.append("test_migration.py")
            logger.info(f"Saved test cases to: {test_path}")
        elif structured_code["tests"]:
            test_path = os.path.join(output_dir, "test_migration.py")
            write_output_file(test_path, structured_code["tests"])
            files_saved.append("test_migration.py")
            logger.info(f"Saved test cases to: {test_path}")
        
        # Save migration plan
        if migration_plan:
            plan_path = os.path.join(output_dir, "migration_plan.md")
            write_output_file(plan_path, f"# Migration Plan\n\n{migration_plan}")
            files_saved.append("migration_plan.md")
            logger.info(f"Saved migration plan to: {plan_path}")
        
        # Save full conversation for reference
        conversation_path = os.path.join(output_dir, "full_conversation.json")
        write_output_file(conversation_path, conversation_messages, is_json=True)
        files_saved.append("full_conversation.json")
        
        # Create a README for the output
        readme_content = f"""# SQL to PySpark Migration Output

## Source File
- **SQL File**: {args.sql_file}
- **Context**: {args.context or 'None provided'}

## Generated Files
{chr(10).join([f"- {file}" for file in files_saved])}

## Architecture Overview
This migration follows the medallion architecture pattern:
1. **Bronze Layer**: Raw data ingestion
2. **Stage 1**: Base data transformations (CustomerBase, TransactionSummary)
3. **Stage 2**: Advanced analytics (RFM scores, Customer metrics)
4. **Gold Layer**: Final aggregated data ready for consumption

## Next Steps
1. Review the migration plan
2. Set up your Microsoft Fabric environment
3. Deploy the PySpark code in the correct order (Bronze → Stage1 → Stage2 → Gold)
4. Run the test cases to validate the migration
"""
        write_output_file(os.path.join(output_dir, "README.md"), readme_content)
        
        print("\n=== SQL Migration Complete ===")
        print(f"PySpark code and migration artifacts saved to: {output_dir}")
        print(f"\nAgents that participated: {', '.join(result_data.get('agents_responded', []))}")
        print("\nGenerated files:")
        for file in sorted(files_saved):
            print(f" - {file}")
        
        if migration_plan:
            print("\nMigration Plan Summary:")
            print("-" * 40)
            print(migration_plan[:500] + "..." if len(migration_plan) > 500 else migration_plan)
        
        logger.info(f"Migration complete. Results saved to {output_dir}")
        
    except FileNotFoundError as e:
        logger.error(f"SQL file not found: {str(e)}")
        print(f"Error: {str(e)}")
        return 1
    except Exception as e:
        logger.error(f"Error migrating SQL: {str(e)}", exc_info=True)
        print(f"Error: {str(e)}")
        return 1
    
    return 0

def interact_with_agent(args):
    """
    Interact with a specific agent.
    
    Args:
        args: Command-line arguments
    """
    try:
        # Initialize agent manager
        agent_manager = AgentManager()
        
        logger.info(f"Initiating interaction with {args.agent}")
        
        # Interact with agent
        response = agent_manager.interact_with_agent(args.agent, args.message)
        
        print(f"\n=== Response from {args.agent} ===")
        print(response)
        
        logger.info(f"Interaction with {args.agent} complete")
        
    except ValueError as e:
        logger.error(f"Invalid agent name: {str(e)}")
        print(f"Error: {str(e)}")
        return 1
    except Exception as e:
        logger.error(f"Error interacting with agent: {str(e)}", exc_info=True)
        print(f"Error: {str(e)}")
        return 1
    
    return 0

def list_agents(args):
    """
    List all available agents.
    
    Args:
        args: Command-line arguments
    """
    agent_descriptions = {
        "BusinessAnalyst": "Analyzes business requirements and identifies key metrics",
        "DomainExpert": "SQL Server expert who analyzes technical patterns and complexity",
        "AzureExpert": "Provides Azure and Microsoft Fabric migration recommendations",
        "ProductOwner": "Creates migration plans and prioritizes deliverables",
        "AzureDataEngineer": "Translates SQL to PySpark with medallion architecture",
        "TechLead": "Reviews code quality and ensures best practices",
        "TestingAgent": "Creates comprehensive test cases for validation"
    }
    
    print("\n=== Available Agents ===\n")
    for agent, description in agent_descriptions.items():
        print(f"  {agent}:")
        print(f"    {description}\n")
    
    print("To interact with an agent, use:")
    print('  python sql_agent_cli.py interact <AgentName> "<your message>"')
    
    return 0