"""Stores parameterized prompt templates for the LLM assistant."""

# Example prompt template - we will refine this later
SQL_TO_PYSPARK_TEMPLATE = """
Translate the following SQL logic into PySpark code.

**Context:**
{context}

**Input Schema (if available):**
{input_schema}

**Output Schema (if available):**
{output_schema}

**SQL Logic:**
```sql
{sql_code}
```

**PySpark Code:**
"""

# Add more specific templates as needed, e.g., for complex CASE statements, window functions, UDFs, etc. 