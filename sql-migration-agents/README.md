## Overview

Repo purpose: Build a multi-agent AI system for migrating SQL Server stored procedures to PySpark code intedend for creating medallion architecture, powered by AutoGen.


## Key Capabilities

Each capability is implemented by one or more specialized agents working together:

- **Business & Technical Analysis**: The Business Analyst and Domain Expert agents analyze SQL code to understand business purpose, data flows, and technical implementation details
- **PySpark Translation**: The Azure Data Engineer agent converts SQL to optimized PySpark code for Microsoft Fabric
- **Medallion Architecture Implementation**: Generated code follows bronze (raw), silver (validated), and gold (business-ready) layer architecture
- **Migration Planning**: The Product Owner agent creates comprehensive migration plans with prioritized tasks
- **Test Generation**: The Testing Agent creates validation tests to ensure functional equivalence
- **Code Review**: The Tech Lead agent provides code quality feedback and optimization recommendations

## Agent Architecture

The system implements a collaborative multi-agent architecture where each agent handles specific aspects of the migration process:

1. **Business Analyst**: Analyzes business requirements, data flows, and metrics calculations
2. **Domain Expert**: Identifies SQL patterns, constructs, and technical implementation details
3. **Azure Expert**: Provides guidance on Microsoft Fabric and Azure data services
4. **Product Owner**: Creates migration plans and prioritizes implementation tasks
5. **Azure Data Engineer**: Translates SQL to PySpark with medallion architecture
6. **Tech Lead**: Reviews code for quality, performance, and maintainability
7. **Testing Agent**: Creates test cases to validate functional equivalence

These agents collaborate through structured conversations, with each contributing their expertise to produce a comprehensive migration solution.

## Installation

### Prerequisites

- Python 3.9 or higher
- OpenAI API key or Azure OpenAI API key
- SQL Server stored procedures to migrate

### Setup

1. Clone the repository:
```bash
git clone https://github.com/yourusername/sql-migration-agents.git
cd sql-migration-agents
```

2. Create and activate a virtual environment:
```bash
# Create a virtual environment
python -m venv venv

# Activate the virtual environment on macOS/Linux:
source venv/bin/activate
```

3. Install dependencies and the package:
```bash
pip install -r requirements.txt
pip install -e .
```

4. Configure API credentials:
```bash
cp .env.template .env
# Edit .env file with your OpenAI or Azure OpenAI credentials
```

## Usage

The tool provides several commands for different migration tasks:

### Analyze SQL Procedure

Performs a detailed analysis of the stored procedure without generating migration code:

```bash
# Example with context
sql-migrate analyze data/input/CustomerRFM.sql --context "Calculates customer RFM segments based on purchase history"

# Example specifying output directory
sql-migrate analyze data/input/AnotherProc.sql --output-dir data/output/another_proc_analysis

# Example with both context and output directory
sql-migrate analyze data/input/ThirdProc.sql --context "Some context" --output-dir data/output/third_proc_analysis
```

This will generate files like:
- Business purpose analysis
- Technical implementation details
- Azure/PySpark migration considerations

### Migrate SQL to PySpark

Performs full migration with the goal of generating Bronze, Silver, and Gold layer code (and potentially intermediate stages):

```bash
# Example specifying output directory
sql-migrate migrate data/input/CustomerRFM.sql --output-dir data/output/rfm_procedure

# Example with context
sql-migrate migrate data/input/AnotherProc.sql --context "Important migration notes"

# Example with both context and output directory
sql-migrate migrate data/input/ThirdProc.sql --context "Context" --output-dir data/output/third_proc_output
```

This will generate files like:
- Bronze layer implementation (raw data ingestion)
- Silver layer implementation (data validation/transformation)
- Gold layer implementation (business metrics)
- Test cases for validation
- Migration plan with implementation steps

### Agent Interaction

Interact directly with a specific agent:

```bash
sql-migrate interact azure_data_engineer "How would you implement a slowly changing dimension type 2 in PySpark?"
```

### List Available Agents

```bash
sql-migrate list-agents
```

## Technical Implementation

### AutoGen Framework

This project leverages the [AutoGen](https://github.com/microsoft/autogen) framework to orchestrate agent collaboration:

- **Group Chat**: Agents collaborate in a structured conversation
- **Specialized Roles**: Each agent has specific expertise and responsibilities
- **Knowledge Sharing**: Information flows between agents to ensure coherent output
- **Coordination**: Managed workflow ensures proper sequencing of analysis, planning, implementation, and review

### Project Structure

```
sql-migration-agents/
├── src/                           # Source code
│   ├── agents/                    # Agent system
│   │   ├── agent_manager.py       # Multi-agent coordination
│   │   ├── prompts.py             # Agent role definitions
│   │   ├── tasks.py               # Task templates
│   │   └── utils.py               # Agent utilities
│   ├── config/                    # Configuration
│   │   ├── llm.py                 # LLM configuration
│   │   └── logging.py             # Logging setup
│   ├── cli/                       # Command line interface
│   │   ├── commands.py            # Command implementation
│   │   └── main.py                # CLI entry point
│   └── utils/                     # Utilities
│       └── file_utils.py          # File handling
├── data/                          # Data directory
│   ├── input/                     # SQL stored procedures
│   └── output/                    # Generated analysis files and PySpark code
├── tests/                         # Unit tests
├── setup.py                       # Package configuration
├── requirements.txt               # Dependencies
└── .env.template                  # Environment configuration
```

## Output Artifacts

The migration process generates several artifacts:

1. **PySpark Implementation**:
   - `bronze_layer.py`: Raw data ingestion from source systems
   - `silver_layer.py`: Data validation, cleaning, and normalization
   - `gold_layer.py`: Business metrics and analytics-ready views

2. **Analysis Documents**:
   - `business_analysis.md`: Business purpose and requirements
   - `technical_analysis.md`: Technical patterns and implementation details
   - `azure_recommendations.md`: Microsoft Fabric optimization guidelines

3. **Quality Assurance**:
   - `test_migration.py`: Validation test cases
   - `migration_plan.md`: Implementation steps and priorities

