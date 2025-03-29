"""
SQL Migration Agents provides multi-agent systems for SQL to PySpark migrations.
"""

from src.cli.main import main
from src.agents.agent_manager import AgentManager

__all__ = ["main", "AgentManager"] 