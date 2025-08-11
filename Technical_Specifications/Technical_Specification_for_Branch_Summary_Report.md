# Technical Specification for Branch Summary Report Enhancement

-=============================================
-Author: Ascendion AVA+
-Date: 
-Description: Integration of BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT
-=============================================

## Introduction

This document outlines the technical specifications for integrating the new source table `BRANCH_OPERATIONAL_DETAILS` into the existing `BRANCH_SUMMARY_REPORT` table. The enhancement aims to improve compliance and audit readiness by incorporating branch-level operational metadata.

## Code Changes

### Impacted Areas
- PySpark ETL logic
- Delta table structure
- Data validation and reconciliation routines

### Logic Changes
1. **Join Logic:**
   - Join `BRANCH_OPERATIONAL_DETAILS` with `BRANCH_SUMMARY_REPORT` using `BRANCH_ID`.
   - Populate `REGION` and `LAST_AUDIT_DATE` columns conditionally based on `IS_ACTIVE = 'Y'.

2. **Backward Compatibility:**
   - Ensure older records without `BRANCH_OPERATIONAL_DETAILS` data remain unaffected.

### Pseudocode
```python
# Load source tables
branch_details = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", "BRANCH_OPERATIONAL_DETAILS").load()
branch_summary = spark.read.format("delta").load("/delta/branch_summary_report")

# Join and populate new columns
updated_summary = branch_summary.join(branch_details, "BRANCH_ID", "left")
updated_summary = updated_summary.withColumn("REGION", when(col("IS_ACTIVE") == 'Y', col("REGION")))
updated_summary = updated_summary.withColumn("LAST_AUDIT_DATE", when(col("IS_ACTIVE") == 'Y', col("LAST_AUDIT_DATE")))

# Write back to Delta table
updated_summary.write.format("delta").mode("overwrite").save("/delta/branch_summary_report")
```

## Data Model Updates

### Source Data Model
**BRANCH_OPERATIONAL_DETAILS**
| Column Name       | Data Type   | Description                   |
|-------------------|-------------|-------------------------------|
| BRANCH_ID         | INT         | Unique identifier for branch  |
| REGION            | VARCHAR(50) | Region name                   |
| MANAGER_NAME      | VARCHAR(100)| Branch manager name           |
| LAST_AUDIT_DATE   | DATE        | Date of last audit            |
| IS_ACTIVE         | CHAR(1)     | Active status ('Y'/'N')       |

### Target Data Model
**BRANCH_SUMMARY_REPORT**
| Column Name       | Data Type   | Description                   |
|-------------------|-------------|-------------------------------|
| BRANCH_ID         | INT         | Unique identifier for branch  |
| BRANCH_NAME       | STRING      | Name of the branch            |
| TOTAL_TRANSACTIONS| BIGINT      | Total number of transactions  |
| TOTAL_AMOUNT      | DOUBLE      | Total transaction amount      |
| REGION            | STRING      | Region name                   |
| LAST_AUDIT_DATE   | DATE        | Date of last audit            |

## Source-to-Target Mapping

| Source Column                     | Target Column                     | Transformation Rule               |
|-----------------------------------|-----------------------------------|-----------------------------------|
| BRANCH_OPERATIONAL_DETAILS.REGION | BRANCH_SUMMARY_REPORT.REGION      | Populate if IS_ACTIVE = 'Y'       |
| BRANCH_OPERATIONAL_DETAILS.LAST_AUDIT_DATE | BRANCH_SUMMARY_REPORT.LAST_AUDIT_DATE | Populate if IS_ACTIVE = 'Y'       |

## Assumptions and Constraints

- The `BRANCH_OPERATIONAL_DETAILS` table is always up-to-date and accurate.
- The `IS_ACTIVE` column determines whether data should be populated in the target table.
- A full reload of the `BRANCH_SUMMARY_REPORT` table is required post-deployment.

## References

- JIRA Story: Extend BRANCH_SUMMARY_REPORT Logic to Integrate New Source Table
- Confluence Documentation: ETL Change - Integration of BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT
- Source DDL: BRANCH_OPERATIONAL_DETAILS
- Target DDL: BRANCH_SUMMARY_REPORT