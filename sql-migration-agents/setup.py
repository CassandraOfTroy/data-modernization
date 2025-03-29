"""
SQL Migration Agents setup script.
"""

from setuptools import setup, find_packages

setup(
    name="sql-migration-agents",
    version="0.1.0",
    description="AutoGen-based multi-agent system for SQL to PySpark migrations",
    author="Data Modernization Team",
    packages=find_packages(),
    install_requires=[
        "pyautogen>=0.2.0",
        "loguru",
        "sqlparse",
        "python-dotenv",
    ],
    entry_points={
        "console_scripts": [
            "sql-migrate=src.cli.main:main",
        ],
    },
    python_requires=">=3.9",
) 