"""
Command handlers for SQL Migration Agents CLI.
"""

import os
from pathlib import Path
from loguru import logger
import sqlparse
from typing import Dict, Any

from src.agents.agent_manager import AgentManager
from src.agents.tasks import get_analysis_task_template, get_migration_task_template
from src.utils.file_utils import read_sql_file, write_output_file

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
        
        # Execute the task
        agent_manager.execute_task(task_message)
        
        # Get the results
        results = agent_manager.get_results_for_analysis()
        
        # Print summary
        print("\n=== SQL Analysis Summary ===")
        print("\nBusiness Analysis:")
        print("-" * 40)
        print(results["business_analysis"][:300] + "..." if len(results["business_analysis"]) > 300 else results["business_analysis"])
        
        print("\nTechnical Analysis:")
        print("-" * 40)
        print(results["technical_analysis"][:300] + "..." if len(results["technical_analysis"]) > 300 else results["technical_analysis"])
        
        print("\nAzure Recommendations:")
        print("-" * 40)
        print(results["azure_recommendations"][:300] + "..." if len(results["azure_recommendations"]) > 300 else results["azure_recommendations"])
        
        # Save results to files
        output_dir = args.output_dir or "data/output/analysis"
        os.makedirs(output_dir, exist_ok=True)
        
        write_output_file(os.path.join(output_dir, "business_analysis.md"), results["business_analysis"])
        write_output_file(os.path.join(output_dir, "technical_analysis.md"), results["technical_analysis"])
        write_output_file(os.path.join(output_dir, "azure_recommendations.md"), results["azure_recommendations"])
        
        # Save full conversation for reference
        write_output_file(os.path.join(output_dir, "full_conversation.json"), results["full_conversation"], is_json=True)
        
        logger.info(f"Analysis complete. Results saved to {output_dir}")
        print(f"\nFull analysis results saved to {output_dir}")
        
    except Exception as e:
        logger.error(f"Error analyzing SQL: {str(e)}")
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
        
        # Execute the task
        agent_manager.execute_task(task_message)
        
        # Get the results
        results = agent_manager.get_results_for_migration()
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Save results to files
        bronze_path = os.path.join(output_dir, "bronze_layer.py")
        silver_path = os.path.join(output_dir, "silver_layer.py")
        gold_path = os.path.join(output_dir, "gold_layer.py")
        tests_path = os.path.join(output_dir, "test_migration.py")
        plan_path = os.path.join(output_dir, "migration_plan.md")
        
        # Save layers if they exist in the code blocks
        pyspark_code = results["pyspark_code"]
        for code_block in pyspark_code:
            if "bronze" in code_block.lower():
                write_output_file(bronze_path, code_block)
                logger.info(f"Saved bronze layer to: {bronze_path}")
            elif "silver" in code_block.lower():
                write_output_file(silver_path, code_block)
                logger.info(f"Saved silver layer to: {silver_path}")
            elif "gold" in code_block.lower():
                write_output_file(gold_path, code_block)
                logger.info(f"Saved gold layer to: {gold_path}")
        
        # Save tests if they exist
        test_cases = results["test_cases"]
        if test_cases:
            write_output_file(tests_path, "\n\n".join(test_cases))
            logger.info(f"Saved test cases to: {tests_path}")
        
        # Save migration plan
        write_output_file(plan_path, results["migration_plan"])
        logger.info(f"Saved migration plan to: {plan_path}")
        
        # Save full conversation for reference
        conversation_path = os.path.join(output_dir, "full_conversation.json")
        write_output_file(conversation_path, results["full_conversation"], is_json=True)
        
        print("\n=== SQL Migration Complete ===")
        print(f"PySpark code and migration artifacts saved to: {output_dir}")
        print("\nGenerated files:")
        for root, _, files in os.walk(output_dir):
            for file in files:
                print(f" - {os.path.relpath(os.path.join(root, file), output_dir)}")
        
        print("\nMigration Plan Summary:")
        print("-" * 40)
        plan = results["migration_plan"]
        print(plan[:300] + "..." if len(plan) > 300 else plan)
        
        logger.info(f"Migration complete. Results saved to {output_dir}")
        
    except Exception as e:
        logger.error(f"Error migrating SQL: {str(e)}")
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
        
        # Interact with agent
        response = agent_manager.interact_with_agent(args.agent, args.message)
        
        print(f"\n=== Response from {args.agent} ===")
        print(response)
        
        logger.info(f"Interaction with {args.agent} complete")
        
    except Exception as e:
        logger.error(f"Error interacting with agent: {str(e)}")
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
        "business_analyst": "Analyzes business requirements and coordinates with technical experts",
        "domain_expert": "SQL Data Engineer with deep domain expertise",
        "azure_expert": "Expert in Azure data services and PySpark",
        "product_owner": "Creates and prioritizes project backlog for migration",
        "azure_data_engineer": "Translates SQL to PySpark with a focus on medallion architecture",
        "tech_lead": "Reviews and improves code quality and architecture",
        "testing_agent": "Creates test cases to validate migrated code"
    }
    
    print("Available agents:")
    for agent, description in agent_descriptions.items():
        print(f"  - {agent}: {description}")
    
    return 0 