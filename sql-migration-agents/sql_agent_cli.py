#!/usr/bin/env python
"""
CLI Entry point for SQL Migration Agents.
"""

import argparse
import sys
import os
from dotenv import load_dotenv
from loguru import logger

# Add src to path to allow imports
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(current_dir, 'src'))

# Import command handlers AFTER potentially modifying sys.path
from cli.commands import analyze_sql, migrate_sql, interact_with_agent, list_agents

def setup_logging():
    """Configure Loguru logging."""
    logger.remove()
    # Console logging - controlled by LOG_LEVEL env var (defaults to INFO)
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logger.add(sys.stderr, level=log_level)
    
    # File logging - if LOG_FILE env var is set
    log_file = os.getenv("LOG_FILE")
    if log_file:
        # Ensure logs directory exists
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)
        logger.add(log_file, level="DEBUG", rotation="10 MB", compression="zip")
        logger.info(f"Logging to file: {log_file}")
    else:
        logger.info("File logging is disabled. Set LOG_FILE environment variable to enable.")

def main():
    """Main CLI entry point."""
    # Load environment variables from .env file
    load_dotenv()
    setup_logging()

    parser = argparse.ArgumentParser(description="SQL Migration Agents CLI")
    subparsers = parser.add_subparsers(dest="command", required=True, help="Available commands")

    # === Analyze Command ===
    parser_analyze = subparsers.add_parser("analyze", help="Analyze a SQL stored procedure")
    parser_analyze.add_argument("--sql-file", required=True, help="Path to the SQL file to analyze")
    parser_analyze.add_argument("--context", default="", help="Optional context about the SQL file")
    parser_analyze.add_argument("--output-dir", help="Directory to save analysis results (default: data/output/analysis)")
    parser_analyze.set_defaults(func=analyze_sql)

    # === Migrate Command ===
    parser_migrate = subparsers.add_parser("migrate", help="Migrate a SQL stored procedure to PySpark")
    parser_migrate.add_argument("--sql-file", required=True, help="Path to the SQL file to migrate")
    parser_migrate.add_argument("--context", default="", help="Optional context about the SQL file")
    parser_migrate.add_argument("--output-dir", help="Directory to save migration artifacts (default: data/output/<sql_file_name>)")
    parser_migrate.set_defaults(func=migrate_sql)
    
    # === Interact Command ===
    parser_interact = subparsers.add_parser("interact", help="Interact directly with a specific agent")
    parser_interact.add_argument("agent", help="Name of the agent to interact with (e.g., BusinessAnalyst)")
    parser_interact.add_argument("message", help="Message to send to the agent")
    parser_interact.set_defaults(func=interact_with_agent)

    # === List Agents Command ===
    parser_list = subparsers.add_parser("list-agents", help="List available agents")
    parser_list.set_defaults(func=list_agents)

    args = parser.parse_args()
    
    # Execute the selected command function
    exit_code = args.func(args)
    sys.exit(exit_code or 0)

if __name__ == "__main__":
    main() 