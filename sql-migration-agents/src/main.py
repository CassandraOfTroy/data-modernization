import os
import asyncio
import argparse
from typing import Dict, Any, Optional, Tuple
import json
from dotenv import load_dotenv
from loguru import logger
import sys

from src.core.agent_manager import AgentManager
from src.utils.project_init import init_project_structure

# Load environment variables
load_dotenv()

# Configure logging
try:
    # Ensure logs directory exists
    os.makedirs("logs", exist_ok=True)
    logger.add("logs/sql_migration.log", rotation="10 MB", retention="1 week")
except Exception as e:
    print(f"Error configuring logging: {e}")
    # Continue with console logging only

def read_sql_file(sql_file: str) -> str:
    """Read SQL code from a file with error handling
    
    Args:
        sql_file: Path to the SQL file to read
        
    Returns:
        The SQL code as a string
        
    Raises:
        FileNotFoundError: If the file doesn't exist
        IOError: If there's an error reading the file
    """
    try:
        with open(sql_file, "r") as f:
            sql_code = f.read()
        return sql_code
    except FileNotFoundError:
        logger.error(f"SQL file not found: {sql_file}")
        raise FileNotFoundError(f"SQL file not found: {sql_file}")
    except IOError as e:
        logger.error(f"Error reading SQL file {sql_file}: {e}")
        raise IOError(f"Error reading SQL file: {e}")

def write_output_file(output_path: str, content: str or Dict, is_json: bool = False) -> bool:
    """Write content to an output file with error handling
    
    Args:
        output_path: Path to the output file
        content: Content to write (string or dict for JSON)
        is_json: Whether to write as JSON
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
        
        with open(output_path, "w") as f:
            if is_json:
                json.dump(content, f, indent=2)
            else:
                f.write(content)
        return True
    except IOError as e:
        logger.error(f"Error writing to {output_path}: {e}")
        return False

async def get_agent_manager() -> AgentManager:
    """Get a singleton instance of the agent manager"""
    if not hasattr(get_agent_manager, "instance"):
        get_agent_manager.instance = AgentManager()
    return get_agent_manager.instance

async def analyze_sql(sql_file: str, business_context: str = "") -> Dict[str, Any]:
    """
    Analyze a SQL file using multiple agents
    
    Args:
        sql_file: Path to the SQL file to analyze
        business_context: Business context about the SQL code
        
    Returns:
        Analysis results from multiple agents
    """
    # Read SQL file
    sql_code = read_sql_file(sql_file)
    
    # Initialize agent manager
    agent_manager = await get_agent_manager()
    
    # Analyze SQL
    logger.info(f"Analyzing SQL file: {sql_file}")
    results = await agent_manager.analyze_stored_procedure(sql_code, business_context)
    
    return results

async def migrate_sql(sql_file: str, output_dir: str, business_context: str = "") -> Dict[str, Any]:
    """
    Migrate a SQL file to PySpark using the full agent workflow
    
    Args:
        sql_file: Path to the SQL file to migrate
        output_dir: Directory to write output files
        business_context: Business context about the SQL code
        
    Returns:
        Results from the migration workflow
    """
    # Read SQL file
    sql_code = read_sql_file(sql_file)
    
    # Initialize agent manager
    agent_manager = await get_agent_manager()
    
    # Run migration workflow
    logger.info(f"Migrating SQL file: {sql_file}")
    results = await agent_manager.run_migration_workflow(sql_code, business_context)
    
    # Save results to output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Save outputs, checking for errors
    output_files = {
        "pyspark_code.py": (results["refactored_code"], False),
        "analysis.json": ({k: v for k, v in results.items() if k != "refactored_code"}, True),
        "test_plan.md": (results["test_plan"], False)
    }
    
    success = True
    for filename, (content, is_json) in output_files.items():
        output_path = os.path.join(output_dir, filename)
        if not write_output_file(output_path, content, is_json):
            success = False
    
    if success:
        logger.info(f"Migration results saved to {output_dir}")
    else:
        logger.warning(f"Some results could not be saved to {output_dir}")
    
    return results

async def interact_with_agent(agent_name: str, prompt: str) -> Optional[str]:
    """
    Interact directly with a specific agent
    
    Args:
        agent_name: Name of the agent to interact with
        prompt: Prompt to send to the agent
        
    Returns:
        Agent's response
    """
    # Initialize agent manager
    agent_manager = await get_agent_manager()
    
    # Get agent
    agent = agent_manager.get_agent(agent_name)
    if not agent:
        logger.error(f"Agent '{agent_name}' not found")
        return None
    
    # Process prompt
    logger.info(f"Sending prompt to agent '{agent_name}'")
    response = await agent.process(prompt)
    
    return response.response

def main():
    """Main entry point for the application"""
    parser = argparse.ArgumentParser(description="SQL Migration Agents")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # Analyze SQL command
    analyze_parser = subparsers.add_parser("analyze", help="Analyze a SQL file")
    analyze_parser.add_argument("sql_file", help="Path to the SQL file to analyze")
    analyze_parser.add_argument("--context", help="Business context about the SQL code", default="")
    analyze_parser.add_argument("--output", help="Output file for analysis results", default="analysis.json")
    
    # Migrate SQL command
    migrate_parser = subparsers.add_parser("migrate", help="Migrate a SQL file to PySpark")
    migrate_parser.add_argument("sql_file", help="Path to the SQL file to migrate")
    migrate_parser.add_argument("--context", help="Business context about the SQL code", default="")
    migrate_parser.add_argument("--output-dir", help="Directory to write output files", default="output")
    
    # Interact with agent command
    interact_parser = subparsers.add_parser("interact", help="Interact with a specific agent")
    interact_parser.add_argument("agent", help="Name of the agent to interact with")
    interact_parser.add_argument("prompt", help="Prompt to send to the agent")
    
    # List agents command
    subparsers.add_parser("list-agents", help="List all available agents")
    
    # Initialize project command
    subparsers.add_parser("init", help="Initialize project structure")
    
    # Parse arguments
    args = parser.parse_args()
    
    # Run appropriate command
    try:
        if args.command == "analyze":
            # Analyze SQL
            results = asyncio.run(analyze_sql(args.sql_file, args.context))
            
            # Save results
            if write_output_file(args.output, results, is_json=True):
                print(f"Analysis results saved to {args.output}")
            else:
                print(f"Error saving analysis results")
                sys.exit(1)
            
        elif args.command == "migrate":
            # Migrate SQL
            try:
                results = asyncio.run(migrate_sql(args.sql_file, args.output_dir, args.context))
                print(f"Migration results saved to {args.output_dir}")
            except (FileNotFoundError, IOError) as e:
                print(f"Error: {e}")
                sys.exit(1)
            
        elif args.command == "interact":
            # Interact with agent
            response = asyncio.run(interact_with_agent(args.agent, args.prompt))
            
            if response:
                print(f"\nResponse from {args.agent}:\n")
                print(response)
            else:
                print(f"Error: Agent '{args.agent}' not found")
                sys.exit(1)
            
        elif args.command == "list-agents":
            # List agents
            agent_manager = asyncio.run(get_agent_manager())
            agents = agent_manager.list_agents()
            
            print("\nAvailable agents:\n")
            for agent in agents:
                print(f"- {agent['name']}: {agent['description']}")
        
        elif args.command == "init":
            # Initialize project structure
            init_project_structure()
            print("Project structure initialized successfully!")
            
        else:
            parser.print_help()
            
    except Exception as e:
        logger.error(f"Error running command {args.command if hasattr(args, 'command') else 'unknown'}: {e}")
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 