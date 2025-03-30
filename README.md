# Data Modernization

An experimental toolkit for speeding up modernization of the legacy data systems to cloud-native architectures on Azure.

## Overview

This repository contains two complementary projects focused on modernizing legacy SQL Server data platforms to modern cloud-native architectures on Microsoft Azure:

1. **SQL to Medallion**: A toolkit for migrating SQL Server stored procedures to a medallion architecture (bronze, silver, gold layers) on Microsoft Fabric.

2. **SQL Migration Agents**: A multi-agent system for analyzing, transforming, and modernizing SQL code using AI-powered agents.

Both projects leverage Azure OpenAI capabilities to assist with code translation, analysis, and transformation.

## Project Structure

```
data-modernization/
├── sql-to-medallion/           # SQL Server to medallion architecture migration toolkit
│   ├── config/                 # Configuration files
│   ├── data/                   # Sample data and schemas
│   ├── notebooks/              # Example notebooks
│   ├── src/                    # Source code
│   │   ├── bronze/             # Bronze layer processing
│   │   ├── silver/             # Silver layer processing
│   │   ├── gold/               # Gold layer processing
│   │   ├── llm/                # LLM integration
│   │   ├── orchestration/      # Pipeline orchestration
│   │   └── utils/              # Utility functions
│   └── requirements.txt        # Project dependencies
│
├── sql-migration-agents/       # Multi-agent system for SQL modernization
│   ├── data/                   # Example SQL files
│   │   ├── input/              # Input SQL files
│   │   └── output/             # Generated output
│   ├── src/
│   │   ├── agents/             # Agent implementations
│   │   ├── cli/                # Command-line interface
│   │   ├── config/             # Configuration
│   │   └── utils/              # Utility functions
│   ├── tests/                  # Unit and integration tests
│   ├── setup.py                # Package setup
│   └── requirements.txt        # Project dependencies
│
└── README.md                   # This file
```

## Key Features

### SQL to Medallion

A toolkit designed to support the migration of legacy SQL Server stored procedures to a modern data lakehouse architecture using the medallion pattern on Fabric.

- **Automated SQL to PySpark Translation**: Convert SQL Server stored procedures to PySpark code
- **Medallion Architecture Implementation**: Organize data processing into bronze, silver, and gold layers
- **Azure OpenAI Integration**: Leverage AI for complex code translation and analysis
- **Comprehensive Logging**: Track transformation processes and data lineage
- **Modular Design**: Separate modules for extraction, transformation, and loading

### SQL Migration Agents

A multi-agent system for modernizing SQL stored procedures and migrating them to modern data platforms like Fabric.

- **Multi-Agent Architecture**: Specialized agents working together to handle complex SQL modernization tasks
- **Azure AI Integration**: Seamless integration with Azure OpenAI services
- **PySpark Translations**: Convert SQL Server stored procedures to PySpark code
- **SQL Analysis**: Analyze and understand complex SQL structures
- **Test Generation**: Automatic generation of test cases
- **Command-line Interface**: Easy to use CLI for integration into existing workflows

## Getting Started

### Prerequisites

- Python 3.8+
- Azure OpenAI API access or OpenAI API access
- SQL Server with JDBC connectivity (for SQL to Medallion)

### SQL to Medallion

See the detailed [SQL to Medallion README](./sql-to-medallion/README.md) for setup instructions and usage examples.

### SQL Migration Agents

See the detailed [SQL Migration Agents README](./sql-migration-agents/README.md) for setup instructions and usage examples.

## Use Cases

### When to use SQL to Medallion

- When migrating a specific set of SQL Server stored procedures to PySpark code
- When implementing a medallion architecture directly
- When working with data engineers who prefer a more hands-on approach
- For smaller migration projects with clear scope and requirements

### When to use SQL Migration Agents

- When dealing with SQL modernization projects
- When automation is a priority for the migration process
- When creating a self-service tool for data teams to use
- When needing detailed analysis and documentation of legacy code
- For generating tests and validation code along with the modernization
