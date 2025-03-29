# SQL Migration Agents

A multi-agent system for migrating SQL Server stored procedures to Microsoft Fabric PySpark with a medallion architecture approach.

## Overview

SQL Migration Agents is a Python-based tool that uses specialized AI agents to automate the migration of SQL Server stored procedures to Microsoft Fabric PySpark code. Each agent represents a specific role in the migration process, collaborating to analyze, translate, review, and test the migrated code.

## Key Capabilities

- Analyze SQL stored procedures for business purpose and technical details
- Translate SQL Server code to PySpark optimized for Microsoft Fabric
- Implement medallion architecture (bronze, silver, gold layers)
- Create comprehensive migration plans and documentation
- Generate test cases to validate functional equivalence
- Provide code reviews and optimization recommendations

## Agent Architecture

The system is built around 7 specialized agents:

1. **Business Analyst**: Analyzes business requirements and coordinates with technical experts
2. **Domain Expert**: SQL Data Engineer with deep domain expertise
3. **Azure Cloud Expert**: Expert in Azure data services and PySpark
4. **Product Owner**: Creates and prioritizes project backlog for migration
5. **Azure Data Engineer**: Translates SQL to PySpark with a focus on medallion architecture
6. **Tech Lead**: Reviews and improves code quality and architecture
7. **Testing Agent**: Creates test cases to validate migrated code

## Quick Start

### Prerequisites

- Python 3.8 or higher
- OpenAI API key
- SQL Server stored procedures to migrate

### Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/sql-migration-agents.git
cd sql-migration-agents
```

2. Install the required dependencies:
```bash
pip install -r requirements.txt
```

3. Set up environment variables by copying `.env.template` to `.env` and filling in your OpenAI API key:
```bash
cp .env.template .env
# Edit .env file with your OpenAI API key
```

4. Initialize the project structure:
```bash
python -m src.utils.project_init
```

### Usage

#### Analyze a SQL Stored Procedure

```bash
python -m src.main analyze data/input/sample_procedure.sql --context "This procedure calculates RFM metrics for customers"
```

#### Migrate a SQL Stored Procedure to PySpark

```bash
python -m src.main migrate data/input/sample_procedure.sql --output-dir data/output/rfm_procedure
```

#### Interact with a Specific Agent

```bash
python -m src.main interact azure_data_engineer "How would you implement a slowly changing dimension type 2 in PySpark?"
```

#### List All Available Agents

```bash
python -m src.main list-agents
```

## Integration with Azure AI Foundry

This project can be integrated with Azure AI Foundry by:

1. Packaging the agents as AI Foundry skills
2. Using AI Foundry for agent orchestration
3. Leveraging AI Foundry's knowledge bases to store domain expertise
4. Integrating with Azure AI Studio for prompt management
5. Connecting to Azure resources via AI Foundry integrations

## Project Structure

```
sql-migration-agents/
├── src/
│   ├── agents/             # Specialized agents
│   │   ├── business_analyst.py
│   │   ├── domain_expert.py
│   │   ├── azure_expert.py
│   │   ├── product_owner.py
│   │   ├── azure_data_engineer.py
│   │   ├── tech_lead.py
│   │   └── testing_agent.py
│   ├── core/               # Core functionality
│   │   ├── base_agent.py
│   │   └── agent_manager.py
│   ├── utils/              # Utility functions
│   │   └── project_init.py
│   └── main.py             # Main entry point
├── data/
│   ├── input/              # Input SQL files
│   └── output/             # Output PySpark files
├── tests/                  # Project tests
├── requirements.txt
├── .env.template
└── README.md
```

## Example Output

When migrating a SQL stored procedure, the system will produce:

1. A complete PySpark implementation with medallion architecture
2. Analysis of the business purpose and technical details
3. A comprehensive test plan
4. Architecture recommendations
5. User stories for implementation planning

