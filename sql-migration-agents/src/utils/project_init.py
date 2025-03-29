import os
import shutil
from pathlib import Path
from loguru import logger

def init_project_structure():
    """Initialize the project structure"""
    try:
        # Get project root directory
        project_root = Path(__file__).parent.parent.parent.absolute()
        
        # Create necessary directories
        directories = [
            "logs",
            "data/input",
            "data/output",
            "tests"
        ]
        
        for directory in directories:
            dir_path = os.path.join(project_root, directory)
            os.makedirs(dir_path, exist_ok=True)
            logger.info(f"Created directory: {dir_path}")
        
        # Create empty __init__.py files to make directories importable
        init_files = [
            "src/__init__.py",
            "src/agents/__init__.py",
            "src/core/__init__.py",
            "src/utils/__init__.py",
            "tests/__init__.py"
        ]
        
        for init_file in init_files:
            file_path = os.path.join(project_root, init_file)
            
            # Check if directory exists, create if not
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            # Create file if it doesn't exist
            if not os.path.exists(file_path):
                try:
                    with open(file_path, "w") as f:
                        f.write("# This file makes the directory a Python package\n")
                    logger.info(f"Created file: {file_path}")
                except IOError as e:
                    logger.error(f"Failed to create file {file_path}: {e}")
        
        # Create a sample SQL file for testing
        sample_sql_path = os.path.join(project_root, "data/input/sample_procedure.sql")
        if not os.path.exists(sample_sql_path):
            sample_sql = """
CREATE PROCEDURE [dbo].[CalculateCustomerRFM]
AS
BEGIN
    SET NOCOUNT ON;
    
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
    
    -- Insert or update the RFM scores in the customer segmentation table
    MERGE INTO Customer.Segmentation AS target
    USING (
        SELECT
            cm.CustomerID,
            cm.Recency,
            cm.Frequency,
            cm.Monetary,
            rfm.R_Score,
            rfm.F_Score,
            rfm.M_Score,
            CONCAT(rfm.R_Score, rfm.F_Score, rfm.M_Score) AS RFM_Score,
            GETDATE() AS CalculatedDate
        FROM
            CustomerMetrics cm
        JOIN
            RFMScores rfm ON cm.CustomerID = rfm.CustomerID
    ) AS source
    ON (target.CustomerID = source.CustomerID)
    WHEN MATCHED THEN
        UPDATE SET
            Recency = source.Recency,
            Frequency = source.Frequency,
            Monetary = source.Monetary,
            R_Score = source.R_Score,
            F_Score = source.F_Score,
            M_Score = source.M_Score,
            RFM_Score = source.RFM_Score,
            LastUpdated = source.CalculatedDate
    WHEN NOT MATCHED THEN
        INSERT (
            CustomerID, Recency, Frequency, Monetary, 
            R_Score, F_Score, M_Score, RFM_Score, LastUpdated
        )
        VALUES (
            source.CustomerID, source.Recency, source.Frequency, source.Monetary,
            source.R_Score, source.F_Score, source.M_Score, source.RFM_Score, source.CalculatedDate
        );
    
    -- Log the completion of the RFM calculation
    INSERT INTO Logs.ProcessLog (ProcessName, StartTime, EndTime, RowsAffected, Status)
    SELECT
        'CalculateCustomerRFM',
        DATEADD(minute, -5, GETDATE()), -- Approximate start time
        GETDATE(),
        @@ROWCOUNT,
        'Success';
END
            """
            
            try:
                with open(sample_sql_path, "w") as f:
                    f.write(sample_sql)
                logger.info(f"Created sample SQL file: {sample_sql_path}")
            except IOError as e:
                logger.error(f"Failed to create sample SQL file: {e}")
        
        # Explicitly ensure logs directory exists and create a .gitkeep file
        logs_dir = os.path.join(project_root, "logs")
        os.makedirs(logs_dir, exist_ok=True)
        
        gitkeep_file = os.path.join(logs_dir, ".gitkeep")
        if not os.path.exists(gitkeep_file):
            try:
                with open(gitkeep_file, "w") as f:
                    pass  # Create empty file
                logger.info(f"Created .gitkeep file in logs directory")
            except IOError as e:
                logger.error(f"Failed to create .gitkeep file: {e}")
        
        return project_root
    
    except Exception as e:
        logger.error(f"Error initializing project structure: {e}")
        raise

if __name__ == "__main__":
    init_project_structure()
    print("Project structure initialized successfully!") 