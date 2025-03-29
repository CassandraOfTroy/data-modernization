from pyspark.sql import SparkSession
import argparse
import datetime
import json
import os
from typing import Dict, Any, Optional, List, Tuple

from ..utils.logging import get_logger
from ..utils.config import APP_CONFIG
from ..utils.spark import get_spark_session
from ..bronze.extract import run_bronze_extraction
from ..silver.transform import run_silver_transformation
from ..gold.transform import run_gold_transformation

logger = get_logger(__name__)

def parse_arguments():
    """Parse command line arguments for the pipeline."""
    parser = argparse.ArgumentParser(description='SQL to Medallion Architecture Pipeline')
    
    parser.add_argument(
        '--process-date',
        type=lambda d: datetime.datetime.strptime(d, '%Y-%m-%d').date(),
        default=None,
        help='Processing date (YYYY-MM-DD). Defaults to yesterday if not provided.'
    )
    
    parser.add_argument(
        '--process-type',
        type=str,
        choices=['FULL', 'INCREMENTAL'],
        default='FULL',
        help='Process type (FULL or INCREMENTAL). Defaults to FULL.'
    )
    
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug mode for additional logging and outputs.'
    )
    
    parser.add_argument(
        '--retention-days',
        type=int,
        default=365,
        help='Retention period in days for historical data. Defaults to 365.'
    )
    
    parser.add_argument(
        '--high-value-threshold',
        type=float,
        default=5000.00,
        help='Threshold for high-value customers. Defaults to 5000.00.'
    )
    
    parser.add_argument(
        '--loyalty-threshold',
        type=int,
        default=5,
        help='Threshold for loyal customers. Defaults to 5.'
    )
    
    # Add churn threshold parameters
    parser.add_argument('--churn-days-very-high', type=int, default=180)
    parser.add_argument('--churn-days-high', type=int, default=90)
    parser.add_argument('--churn-days-medium', type=int, default=60)
    parser.add_argument('--churn-days-low', type=int, default=30)
    
    return parser.parse_args()

def run_pipeline(
    process_date: Optional[datetime.date] = None,
    process_type: str = 'FULL',
    debug_mode: bool = False,
    retention_period_days: int = 365,
    high_value_threshold: float = 5000.00,
    loyalty_threshold: int = 5,
    churn_days_very_high: int = 180,
    churn_days_high: int = 90,
    churn_days_medium: int = 60,
    churn_days_low: int = 30
) -> Dict[str, Any]:
    """Main function to run the entire SQL to Medallion pipeline.
    
    Args:
        process_date: Date for which to process data. Defaults to yesterday.
        process_type: Process type ('FULL' or 'INCREMENTAL').
        debug_mode: Whether to run in debug mode (more verbose output).
        retention_period_days: Number of days to retain historical data.
        high_value_threshold: Threshold for high-value customers.
        loyalty_threshold: Threshold for loyal customers.
        churn_days_*: Thresholds for churn probability calculations.
    
    Returns:
        Dictionary containing information about the pipeline run.
    """
    # Initialize result dictionary
    result = {
        "status": "Success",
        "layers": {
            "bronze": {"status": "Not Run", "tables": []},
            "silver": {"status": "Not Run", "tables": []},
            "gold": {"status": "Not Run", "tables": []}
        },
        "error": None,
        "started_at": datetime.datetime.now().isoformat(),
        "completed_at": None
    }
    
    # Set default process date if not provided
    if process_date is None:
        process_date = datetime.date.today() - datetime.timedelta(days=1)
    
    # Record pipeline start time
    processing_start_time = datetime.datetime.now()
    
    logger.info(f"Starting SQL to Medallion pipeline for {process_date}")
    logger.info(f"Process Type: {process_type}")
    logger.info(f"Debug Mode: {debug_mode}")
    
    try:
        # Initialize SparkSession
        app_name = f"SQLtoMedallion_{process_date.strftime('%Y%m%d')}_{process_type}"
        spark = get_spark_session(app_name)
        
        # 1. Bronze Layer - Extract source data
        logger.info("=== BRONZE LAYER EXTRACTION ===")
        bronze_dfs = run_bronze_extraction(spark, process_date)
        
        # Update results
        result["layers"]["bronze"]["status"] = "Success"
        result["layers"]["bronze"]["tables"] = list(bronze_dfs.keys())
        
        # 2. Silver Layer - Transform data
        logger.info("=== SILVER LAYER TRANSFORMATION ===")
        silver_dfs = run_silver_transformation(spark, bronze_dfs, process_date, process_type)
        
        # Update results
        result["layers"]["silver"]["status"] = "Success"
        result["layers"]["silver"]["tables"] = list(silver_dfs.keys())
        
        # 3. Gold Layer - Calculate metrics and segments
        logger.info("=== GOLD LAYER TRANSFORMATION ===")
        gold_dfs = run_gold_transformation(
            spark, 
            silver_dfs, 
            process_date,
            process_type,
            processing_start_time,
            high_value_threshold,
            loyalty_threshold,
            churn_days_very_high,
            churn_days_high,
            churn_days_medium,
            churn_days_low,
            retention_period_days,
            debug_mode
        )
        
        # Update results
        result["layers"]["gold"]["status"] = "Success"
        result["layers"]["gold"]["tables"] = list(gold_dfs.keys())
        
        # Print summary statistics (from CustomerAnalyticsSummary)
        if "CustomerAnalyticsSummary" in gold_dfs and gold_dfs["CustomerAnalyticsSummary"] is not None:
            summary_df = gold_dfs["CustomerAnalyticsSummary"]
            # Collect only the first row (in case there are multiple)
            summary_row = summary_df.limit(1).collect()
            if summary_row:
                summary = summary_row[0].asDict()
                logger.info("=== PIPELINE SUMMARY ===")
                logger.info(f"Total Customers: {summary.get('TotalCustomers', 0)}")
                logger.info(f"Active Customers: {summary.get('ActiveCustomers', 0)}")
                logger.info(f"New Customers: {summary.get('NewCustomers', 0)}")
                logger.info(f"Churning Customers: {summary.get('ChurningCustomers', 0)}")
                logger.info(f"High Value Customers: {summary.get('HighValueCustomers', 0)}")
                logger.info(f"Average CLV: ${summary.get('AverageLifetimeValue', 0):.2f}")
        
        logger.info("Pipeline completed successfully.")
    
    except Exception as e:
        logger.error(f"Error in pipeline: {e}", exc_info=True)
        result["status"] = "Failed"
        result["error"] = str(e)
        
        # Update layer status based on where the failure occurred
        if result["layers"]["bronze"]["status"] == "Success" and result["layers"]["silver"]["status"] == "Not Run":
            # Failed at silver layer
            result["layers"]["silver"]["status"] = "Failed"
        elif result["layers"]["silver"]["status"] == "Success" and result["layers"]["gold"]["status"] == "Not Run":
            # Failed at gold layer
            result["layers"]["gold"]["status"] = "Failed"
        elif result["layers"]["bronze"]["status"] == "Not Run":
            # Failed at bronze layer
            result["layers"]["bronze"]["status"] = "Failed"
    
    finally:
        # Finalize result
        result["completed_at"] = datetime.datetime.now().isoformat()
        
        # Calculate duration
        start_time = datetime.datetime.fromisoformat(result["started_at"])
        end_time = datetime.datetime.fromisoformat(result["completed_at"])
        duration_sec = (end_time - start_time).total_seconds()
        result["duration_seconds"] = duration_sec
        
        # Print completion message
        logger.info(f"Pipeline {result['status']} in {duration_sec:.2f} seconds")
        
        # If in debug mode, save the result to a file
        if debug_mode:
            debug_output_path = os.path.join(
                os.path.dirname(__file__), 
                '..', '..', '..',
                'sql-to-medallion', 'debug_output', 
                f"pipeline_result_{process_date}_{process_type}.json"
            )
            os.makedirs(os.path.dirname(debug_output_path), exist_ok=True)
            
            with open(debug_output_path, 'w') as f:
                json.dump(result, f, indent=2)
            
            logger.info(f"Debug output saved to: {debug_output_path}")
    
    return result

def main():
    """Main entry point for the pipeline script."""
    args = parse_arguments()
    
    if args.process_date is None:
        args.process_date = datetime.date.today() - datetime.timedelta(days=1)
    
    result = run_pipeline(
        process_date=args.process_date,
        process_type=args.process_type,
        debug_mode=args.debug,
        retention_period_days=args.retention_days,
        high_value_threshold=args.high_value_threshold,
        loyalty_threshold=args.loyalty_threshold,
        churn_days_very_high=args.churn_days_very_high,
        churn_days_high=args.churn_days_high,
        churn_days_medium=args.churn_days_medium,
        churn_days_low=args.churn_days_low
    )
    
    if result["status"] == "Failed":
        exit(1)
    else:
        exit(0)

if __name__ == "__main__":
    main() 