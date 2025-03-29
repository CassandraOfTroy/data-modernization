import os
import requests
import json
import re
from typing import Dict, Any, Optional, List

from ..utils.logging import get_logger
from ..utils.config import APP_CONFIG
from ...config.prompts import SQL_TO_PYSPARK_TEMPLATE

logger = get_logger(__name__)

def call_azure_openai_api(
    prompt: str,
    max_tokens: int = 1500,
    temperature: float = 0.2,
    assistant_instructions: Optional[str] = None
) -> str:
    """Calls the Azure OpenAI API with the given prompt.

    Args:
        prompt: The prompt to send to the API.
        max_tokens: The maximum number of tokens to generate.
        temperature: The temperature to use for generation.
        assistant_instructions: Optional instructions to provide to the assistant.

    Returns:
        The generated response text.
    """
    openai_config = APP_CONFIG.get('openai', {})
    endpoint = openai_config.get('endpoint')
    api_key = openai_config.get('api_key')
    api_version = openai_config.get('api_version', '2024-02-15-preview')
    deployment_name = openai_config.get('deployment_name')

    if not all([endpoint, api_key, deployment_name]):
        logger.error("Missing required Azure OpenAI configuration.")
        raise ValueError("Missing required Azure OpenAI configuration.")

    url = f"{endpoint}/openai/deployments/{deployment_name}/chat/completions?api-version={api_version}"
    
    headers = {
        "Content-Type": "application/json",
        "api-key": api_key
    }
    
    # Prepare the message content
    messages = [
        {"role": "user", "content": prompt}
    ]
    
    # Add system message if assistant instructions are provided
    if assistant_instructions:
        messages.insert(0, {"role": "system", "content": assistant_instructions})
    
    # Prepare the payload
    payload = {
        "messages": messages,
        "max_tokens": max_tokens,
        "temperature": temperature,
        "top_p": 0.95,
        "frequency_penalty": 0,
        "presence_penalty": 0
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()  # Raise exception for HTTP errors
        
        response_json = response.json()
        message_content = response_json['choices'][0]['message']['content']
        
        return message_content
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Error calling Azure OpenAI API: {e}", exc_info=True)
        raise

def clean_pyspark_code(code_text: str) -> str:
    """Cleans the PySpark code returned by the LLM.
    
    Extracts the actual code from markdown code blocks and removes
    unnecessary explanations or formatting.
    
    Args:
        code_text: The raw text returned by the LLM.
        
    Returns:
        The cleaned PySpark code.
    """
    # Try to extract code from markdown code blocks
    code_block_pattern = r"```(?:python|pyspark)?\s*([\s\S]*?)```"
    code_blocks = re.findall(code_block_pattern, code_text)
    
    if code_blocks:
        # If multiple code blocks found, join them
        return "\n\n".join(code_blocks).strip()
    else:
        # If no code blocks found, return the original text (LLM might not have used markdown)
        return code_text.strip()

def translate_sql_to_pyspark(
    sql_code: str,
    context: str = "",
    input_schema: Optional[Dict[str, List[Dict[str, str]]]] = None,
    output_schema: Optional[Dict[str, List[Dict[str, str]]]] = None
) -> str:
    """Translates SQL code to PySpark using the Azure OpenAI service.
    
    Args:
        sql_code: The SQL code to translate.
        context: Additional context about the SQL code.
        input_schema: Optional schema information for input tables.
        output_schema: Optional schema information for output tables.
        
    Returns:
        The translated PySpark code.
    """
    logger.info("Translating SQL to PySpark via Azure OpenAI")
    
    # Convert schema dictionaries to formatted strings
    input_schema_str = json.dumps(input_schema, indent=2) if input_schema else "Not provided"
    output_schema_str = json.dumps(output_schema, indent=2) if output_schema else "Not provided"
    
    # Format the prompt using the template
    prompt = SQL_TO_PYSPARK_TEMPLATE.format(
        context=context,
        input_schema=input_schema_str,
        output_schema=output_schema_str,
        sql_code=sql_code
    )
    
    # Get LLM config
    llm_config = APP_CONFIG.get('llm', {})
    model_params = llm_config.get('model_parameters', {})
    assistant_instructions = llm_config.get('assistant_instructions', "")
    
    # Call the Azure OpenAI API
    response = call_azure_openai_api(
        prompt=prompt,
        max_tokens=model_params.get('max_tokens', 1500),
        temperature=model_params.get('temperature', 0.2),
        assistant_instructions=assistant_instructions
    )
    
    # Clean and return the code
    pyspark_code = clean_pyspark_code(response)
    logger.info("Successfully translated SQL to PySpark")
    
    return pyspark_code

def extract_table_schema(
    spark,
    table_name: str,
    source_type: str = "jdbc"
) -> Dict[str, List[Dict[str, str]]]:
    """Extracts the schema of a table for use in LLM context.
    
    Args:
        spark: The active SparkSession.
        table_name: The name of the table.
        source_type: The type of source ("jdbc" or "delta").
        
    Returns:
        A dictionary with table name and column definitions.
    """
    logger.info(f"Extracting schema for table: {table_name}")
    
    try:
        if source_type == "jdbc":
            from ..utils.database import read_sql_server_table
            df = read_sql_server_table(spark, table_name)
        elif source_type == "delta":
            # Construct path to Delta table
            if table_name.startswith("Bronze."):
                from ..bronze.extract import get_bronze_path
                path = get_bronze_path(table_name.replace("Bronze.", ""))
            elif table_name.startswith("Silver."):
                from ..silver.transform import get_silver_path
                path = get_silver_path(table_name.replace("Silver.", ""))
            elif table_name.startswith("Gold."):
                from ..gold.transform import get_gold_path
                path = get_gold_path(table_name.replace("Gold.", ""))
            else:
                raise ValueError(f"Unknown Delta table prefix: {table_name}")
                
            df = spark.read.format("delta").load(path)
        else:
            raise ValueError(f"Unsupported source type: {source_type}")
            
        if df is None:
            logger.warning(f"Failed to read table: {table_name}")
            return {table_name: []}
            
        # Convert DataFrame schema to a list of dictionaries
        columns = []
        for field in df.schema.fields:
            columns.append({
                "name": field.name,
                "type": str(field.dataType),
                "nullable": field.nullable
            })
            
        return {table_name: columns}
        
    except Exception as e:
        logger.error(f"Error extracting schema for table {table_name}: {e}", exc_info=True)
        return {table_name: []}

def analyze_sql_code(sql_code: str) -> Dict[str, Any]:
    """Analyzes SQL code to extract table names, CTEs, and other structures.
    
    This is a simple parser to help provide context to the LLM.
    For complex SQL, it's recommended to use a proper SQL parser.
    
    Args:
        sql_code: The SQL code to analyze.
        
    Returns:
        A dictionary with information about the SQL code structure.
    """
    # Simple regex patterns to extract information
    table_pattern = r'FROM\s+([a-zA-Z0-9_\.]+)'
    join_pattern = r'JOIN\s+([a-zA-Z0-9_\.]+)'
    cte_pattern = r'WITH\s+([a-zA-Z0-9_]+)\s+AS'
    
    tables = re.findall(table_pattern, sql_code, re.IGNORECASE)
    joined_tables = re.findall(join_pattern, sql_code, re.IGNORECASE)
    ctes = re.findall(cte_pattern, sql_code, re.IGNORECASE)
    
    # Combine all referenced tables
    all_tables = list(set(tables + joined_tables))
    
    return {
        "referenced_tables": all_tables,
        "ctes": ctes,
        "has_group_by": "GROUP BY" in sql_code.upper(),
        "has_order_by": "ORDER BY" in sql_code.upper(),
        "has_window_function": "OVER (" in sql_code.upper(),
        "has_subquery": "SELECT" in sql_code.upper().split("FROM")[0]
    } 