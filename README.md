# Data Modernization

A comprehensive toolkit for modernizing legacy data systems to cloud-native architectures on Azure.

## Overview

This repository contains two complementary projects focused on modernizing legacy SQL Server data platforms to modern cloud-native architectures on Microsoft Azure:

1. **SQL to Medallion**: A toolkit for migrating SQL Server stored procedures to a medallion architecture (bronze, silver, gold layers) on Azure Databricks.

2. **Data Team Agents**: A multi-agent system for analyzing, transforming, and modernizing SQL code using AI-powered agents.

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
│   │   └── utils/              # Utility functions
│   ├── tests/                  # Unit and integration tests
│   └── README.md               # Project documentation
│
├── data-team-agents/           # Multi-agent system for data modernization
│   ├── data/                   # Example SQL files
│   ├── infrastructure/         # Azure infrastructure as code (Bicep)
│   ├── notebooks/              # Example notebooks
│   ├── prompts/                # LLM prompts for agents
│   ├── src/
│   │   ├── agents/             # Agent implementations
│   │   ├── api/                # API implementation
│   │   ├── models/             # Data models
│   │   └── utils/              # Utility functions
│   ├── tests/                  # Unit and integration tests
│   └── README.md               # Project documentation
│
└── README.md                   # This file
```

## Key Features

### SQL to Medallion

A toolkit designed to streamline the migration of legacy SQL Server stored procedures to a modern data lakehouse architecture using the medallion pattern on Azure Databricks.

- **Automated SQL to PySpark Translation**: Convert SQL Server stored procedures to PySpark code
- **Medallion Architecture Implementation**: Organize data processing into bronze, silver, and gold layers
- **Azure OpenAI Integration**: Leverage AI for complex code translation and analysis
- **Comprehensive Logging**: Track transformation processes and data lineage
- **Modular Design**: Separate modules for extraction, transformation, and loading

### Data Team Agents

A multi-agent system for modernizing SQL stored procedures and migrating them to modern data platforms like Azure Databricks or Azure Synapse Analytics.

- **Multi-Agent Architecture**: Specialized agents working together to handle complex SQL modernization tasks
- **Azure AI Integration**: Seamless integration with Azure OpenAI services
- **PySpark Translations**: Convert SQL Server stored procedures to PySpark code for Azure Databricks
- **Synapse SQL Translations**: Convert SQL Server stored procedures to Azure Synapse SQL
- **Test Generation**: Automatic generation of test cases and synthetic test data
- **API-First Design**: RESTful API for easy integration into existing workflows

## Getting Started

### Prerequisites

- Python 3.8+
- Azure Databricks workspace
- Azure OpenAI API access
- SQL Server with JDBC connectivity

### SQL to Medallion

See the detailed [SQL to Medallion README](./sql-to-medallion/README.md) for setup instructions and usage examples.

### Data Team Agents

See the detailed [Data Team Agents README](./data-team-agents/README.md) for setup instructions and usage examples.

## Use Cases

### When to use SQL to Medallion

- When migrating a specific set of SQL Server stored procedures to Azure Databricks
- When implementing a medallion architecture directly
- When working with data engineers who prefer a more hands-on approach
- For smaller migration projects with clear scope and requirements

### When to use Data Team Agents

- When dealing with large, complex SQL modernization projects
- When automation is a priority for the migration process
- When creating a self-service tool for data teams to use
- When needing detailed analysis and documentation of legacy code
- For generating tests and validation code along with the modernization

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details. 