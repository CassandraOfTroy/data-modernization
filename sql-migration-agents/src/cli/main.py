"""
Main entry point for the SQL Migration Agents CLI.
"""

import sys
import argparse
from dotenv import load_dotenv

from src.config.logging import setup_logger
from src.cli.commands import (
    analyze_sql,
    migrate_sql,
    interact_with_agent,
    list_agents
)

def main():
    """Main entry point for the CLI."""
    # Load environment variables
    load_dotenv()
    
    # Set up logging
    setup_logger()
    
    # Create argument parser
    parser = argparse.ArgumentParser(description="SQL Migration Agents - Migrate SQL to PySpark using AutoGen")
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    
    # Analyze command
    analyze_parser = subparsers.add_parser("analyze", help="Analyze a SQL stored procedure")
    analyze_parser.add_argument("sql_file", help="Path to the SQL file to analyze")
    analyze_parser.add_argument("--context", help="Additional business context for the procedure", default="")
    analyze_parser.add_argument("--output-dir", help="Directory to save analysis results")
    analyze_parser.set_defaults(func=analyze_sql)
    
    # Migrate command
    migrate_parser = subparsers.add_parser("migrate", help="Migrate a SQL stored procedure to PySpark")
    migrate_parser.add_argument("sql_file", help="Path to the SQL file to migrate")
    migrate_parser.add_argument("--output-dir", help="Directory to save migration results")
    migrate_parser.add_argument("--context", help="Additional business context for the procedure", default="")
    migrate_parser.set_defaults(func=migrate_sql)
    
    # Interact command
    interact_parser = subparsers.add_parser("interact", help="Interact with a specific agent")
    interact_parser.add_argument("agent", help="Name of the agent to interact with")
    interact_parser.add_argument("message", help="Message to send to the agent")
    interact_parser.set_defaults(func=interact_with_agent)
    
    # List agents command
    list_parser = subparsers.add_parser("list-agents", help="List all available agents")
    list_parser.set_defaults(func=list_agents)
    
    # Parse arguments
    args = parser.parse_args()
    
    # Execute command
    if args.command is None:
        parser.print_help()
        return 1
    
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main()) 