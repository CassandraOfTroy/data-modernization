#!/usr/bin/env python3
"""
Simple initialization script for the SQL Migration Agents project.
This creates all necessary directories and sample files.
"""

import os
import sys
import shutil
from pathlib import Path

def create_file(file_path, content):
    """Helper function to create a file with error handling"""
    try:
        with open(file_path, "w") as f:
            f.write(content)
        print(f"Created file: {file_path}")
        return True
    except IOError as e:
        print(f"Failed to create file {file_path}: {e}")
        return False

def init_project():
    """Initialize the project structure"""
    print("Initializing SQL Migration Agents project...")
    
    # Get project root directory
    project_root = Path(__file__).parent.absolute()
    print(f"Project root: {project_root}")
    
    # Create necessary directories
    directories = [
        "logs",
        "data/input",
        "data/output",
        "config"
    ]
    
    for directory in directories:
        dir_path = os.path.join(project_root, directory)
        os.makedirs(dir_path, exist_ok=True)
        print(f"Created directory: {dir_path}")
    
    # Create a sample SQL file for testing
    sample_sql_path = os.path.join(project_root, "data/input/sample_procedure.sql")
    if not os.path.exists(sample_sql_path):
        sample_sql = """
CREATE OR ALTER PROCEDURE [dbo].[CalculateCustomerRFM]
    @DataDate DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Set default date if not provided
    IF @DataDate IS NULL
        SET @DataDate = DATEADD(DAY, -1, CAST(GETDATE() AS DATE));
    
    -- Calculate Recency, Frequency, and Monetary metrics for all customers
    WITH CustomerMetrics AS (
        SELECT
            CustomerID,
            DATEDIFF(day, MAX(OrderDate), GETDATE()) AS Recency,
            COUNT(DISTINCT OrderID) AS Frequency,
            SUM(TotalAmount) AS Monetary
        FROM
            Sales.Orders
        WHERE
            OrderDate >= DATEADD(year, -2, GETDATE())
        GROUP BY
            CustomerID
    ),
    -- Calculate percentiles for RFM metrics
    RFMScores AS (
        SELECT
            CustomerID,
            NTILE(5) OVER (ORDER BY Recency DESC) AS R_Score,
            NTILE(5) OVER (ORDER BY Frequency ASC) AS F_Score,
            NTILE(5) OVER (ORDER BY Monetary ASC) AS M_Score
        FROM
            CustomerMetrics
    )
    
    -- Insert results into the RFM table
    INSERT INTO Customer.RFM_Results (
        CustomerID, ProcessDate, Recency, Frequency, Monetary, 
        R_Score, F_Score, M_Score, RFM_Score
    )
    SELECT
        cm.CustomerID,
        @DataDate,
        cm.Recency,
        cm.Frequency,
        cm.Monetary,
        rfm.R_Score,
        rfm.F_Score,
        rfm.M_Score,
        CAST(CONCAT(rfm.R_Score, rfm.F_Score, rfm.M_Score) AS INT) AS RFM_Score
    FROM
        CustomerMetrics cm
    JOIN
        RFMScores rfm ON cm.CustomerID = rfm.CustomerID;
END
        """
        create_file(sample_sql_path, sample_sql)
    
    # Create a sample config file
    sample_config_path = os.path.join(project_root, "config/settings.yaml")
    if not os.path.exists(sample_config_path):
        sample_config = """# SQL Migration Agents Configuration

# Azure OpenAI settings
azure_openai:
  deployment_name: your-deployment-name
  model: gpt-4
  temperature: 0.2
  max_tokens: 4000
  api_version: 2023-12-01-preview

# Azure Key Vault settings (optional)
key_vault:
  enabled: false
  vault_name: your-keyvault-name
  use_managed_identity: true  # Set to false to use client credentials

# Azure Application Insights (optional)
app_insights:
  enabled: false

# Logging settings
logging:
  level: INFO
  file: logs/sql_migration.log

# Migration settings
migration:
  source_dialect: TSQL
  target: PySpark
  implement_medallion: true
  
# Agent settings
agents:
  business_analyst:
    enabled: true
  domain_expert:
    enabled: true
  azure_expert:
    enabled: true
  product_owner:
    enabled: true
  azure_data_engineer:
    enabled: true
  tech_lead:
    enabled: true
  testing_agent:
    enabled: true
"""
        create_file(sample_config_path, sample_config)
    
    # Create an empty .env file if it doesn't exist
    env_file_path = os.path.join(project_root, ".env")
    if not os.path.exists(env_file_path):
        env_template_path = os.path.join(project_root, ".env.template")
        if os.path.exists(env_template_path):
            try:
                with open(env_template_path, "r") as template_file:
                    env_content = template_file.read()
                create_file(env_file_path, env_content)
            except IOError as e:
                print(f"Failed to read .env.template: {e}")
                # Create a basic .env file
                basic_env = """# Basic Azure OpenAI configuration
AZURE_OPENAI_API_KEY=your-azure-openai-key-here
AZURE_OPENAI_ENDPOINT=https://your-resource-name.openai.azure.com/
AZURE_OPENAI_DEPLOYMENT_NAME=your-deployment-name
AZURE_OPENAI_API_VERSION=2023-12-01-preview
AZURE_OPENAI_MODEL=gpt-4

# Project Configuration
PROJECT_NAME=SQLMigration
SOURCE_DIALECT=TSQL
TARGET_DIALECT=PySpark
"""
                create_file(env_file_path, basic_env)
    
    print("Project initialization complete!")
    print("\nYou can now:")
    print("1. Set up your Azure OpenAI API key in .env file")
    print("2. Place your SQL files in the data/input directory")
    print("3. Run the migration tool with: python3 -m sql-migration-agents.src.main")


if __name__ == "__main__":
    init_project() 