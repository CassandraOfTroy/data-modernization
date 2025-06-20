# SQL to PySpark Migration Output

## Source File
- **SQL File**: data/input/CustomerRFM.sql
- **Context**: None provided

## Generated Files
- migration_plan.md
- full_conversation.json

## Architecture Overview
This migration follows the medallion architecture pattern:
1. **Bronze Layer**: Raw data ingestion
2. **Stage 1**: Base data transformations (CustomerBase, TransactionSummary)
3. **Stage 2**: Advanced analytics (RFM scores, Customer metrics)
4. **Gold Layer**: Final aggregated data ready for consumption

## Next Steps
1. Review the migration plan
2. Set up your Microsoft Fabric environment
3. Deploy the PySpark code in the correct order (Bronze → Stage1 → Stage2 → Gold)
4. Run the test cases to validate the migration
