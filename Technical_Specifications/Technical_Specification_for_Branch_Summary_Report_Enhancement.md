=============================================
Author: Ascendion AVA+
Date: 
Description: Technical specification for integrating BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT
=============================================

# Technical Specification for Branch Summary Report Enhancement

## Introduction
This document outlines the technical specifications for integrating the new source table, BRANCH_OPERATIONAL_DETAILS, into the existing BRANCH_SUMMARY_REPORT table. The enhancement aims to incorporate branch-level operational metadata for improved compliance and audit readiness.

## Code Changes
### Impacted Areas
- PySpark ETL logic
- Delta table structure
- Data validation and reconciliation routines

### Logic and Functionality Changes
1. **Join Logic:**
   - Join BRANCH_OPERATIONAL_DETAILS with BRANCH_SUMMARY_REPORT using BRANCH_ID.
   - Populate REGION and LAST_AUDIT_DATE columns conditionally based on IS_ACTIVE = 'Y'.

2. **Backward Compatibility:**
   - Ensure older records in BRANCH_SUMMARY_REPORT remain unaffected.

### Pseudocode
```python
# PySpark pseudocode for integration
branch_details = spark.read.format("jdbc").options(
    url="jdbc:oracle:thin:@<DB_HOST>:<DB_PORT>/<DB_NAME>",
    dbtable="BRANCH_OPERATIONAL_DETAILS",
    user="<USER>",
    password="<PASSWORD>"
).load()

branch_summary = spark.read.format("delta").load("/delta/branch_summary_report")

updated_summary = branch_summary.join(branch_details, "BRANCH_ID", "left")
updated_summary = updated_summary.withColumn(
    "REGION", when(col("IS_ACTIVE") == "Y", col("REGION")).otherwise(None)
).withColumn(
    "LAST_AUDIT_DATE", when(col("IS_ACTIVE") == "Y", col("LAST_AUDIT_DATE")).otherwise(None)
)
updated_summary.write.format("delta").mode("overwrite").save("/delta/branch_summary_report")
```

## Data Model Updates
### Source Data Model
#### BRANCH_OPERATIONAL_DETAILS
| Column Name       | Data Type   | Description                     |
|-------------------|-------------|---------------------------------|
| BRANCH_ID         | INT         | Unique identifier for branch    |
| REGION            | VARCHAR(50) | Region of the branch            |
| MANAGER_NAME      | VARCHAR(100)| Name of the branch manager      |
| LAST_AUDIT_DATE   | DATE        | Date of the last audit          |
| IS_ACTIVE         | CHAR(1)     | Active status of the branch     |

### Target Data Model
#### BRANCH_SUMMARY_REPORT
| Column Name       | Data Type   | Description                     |
|-------------------|-------------|---------------------------------|
| BRANCH_ID         | INT         | Unique identifier for branch    |
| BRANCH_NAME       | STRING      | Name of the branch              |
| TOTAL_TRANSACTIONS| BIGINT      | Total number of transactions    |
| TOTAL_AMOUNT      | DOUBLE      | Total transaction amount        |
| REGION            | STRING      | Region of the branch            |
| LAST_AUDIT_DATE   | DATE        | Date of the last audit          |

## Source-to-Target Mapping
| Source Column                  | Target Column                  | Transformation Rule                          |
|--------------------------------|---------------------------------|---------------------------------------------|
| BRANCH_OPERATIONAL_DETAILS.REGION | BRANCH_SUMMARY_REPORT.REGION | Populate if IS_ACTIVE = 'Y'                 |
| BRANCH_OPERATIONAL_DETAILS.LAST_AUDIT_DATE | BRANCH_SUMMARY_REPORT.LAST_AUDIT_DATE | Populate if IS_ACTIVE = 'Y' |

## Assumptions and Constraints
- BRANCH_OPERATIONAL_DETAILS table is available and accessible via Oracle database.
- BRANCH_SUMMARY_REPORT table is stored in Delta format in Databricks.
- Full reload of BRANCH_SUMMARY_REPORT is required post-deployment.
- Data governance and security standards are adhered to.

## References
- JIRA Story: Extend BRANCH_SUMMARY_REPORT Logic to Integrate New Source Table
- Confluence Documentation: ETL Change - Integration of BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT
- Source Table DDL: BRANCH_OPERATIONAL_DETAILS
- Target Table DDL: BRANCH_SUMMARY_REPORT