=============================================
Author: Ascendion AVA+
Date: <Leave it blank>
Description: Technical specification for integrating BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT
=============================================

# Technical Specification for BRANCH_SUMMARY_REPORT Enhancement

## Introduction
This document outlines the technical specifications for integrating the new source table `BRANCH_OPERATIONAL_DETAILS` into the `BRANCH_SUMMARY_REPORT` table. The enhancement aims to improve compliance and audit readiness by incorporating branch-level operational metadata.

## Code Changes
### Impacted Modules
- PySpark ETL logic for `BRANCH_SUMMARY_REPORT`
- Data validation and reconciliation routines

### Logic Changes
1. **Join Logic**:
   - Join `BRANCH_OPERATIONAL_DETAILS` with `BRANCH_SUMMARY_REPORT` using `BRANCH_ID`.
2. **Conditional Population**:
   - Populate `REGION` and `LAST_AUDIT_DATE` columns based on `IS_ACTIVE = 'Y'`.

### Pseudocode
```python
# Join BRANCH_OPERATIONAL_DETAILS with BRANCH_SUMMARY_REPORT
branch_summary = branch_summary.join(branch_operational_details, "BRANCH_ID")

# Populate new columns conditionally
branch_summary = branch_summary.withColumn(
    "REGION", when(branch_operational_details["IS_ACTIVE"] == 'Y', branch_operational_details["REGION"])
).withColumn(
    "LAST_AUDIT_DATE", when(branch_operational_details["IS_ACTIVE"] == 'Y', branch_operational_details["LAST_AUDIT_DATE"])
)
```

## Data Model Updates
### Source Data Model
#### BRANCH_OPERATIONAL_DETAILS
| Column Name       | Data Type   | Description                      |
|-------------------|-------------|----------------------------------|
| BRANCH_ID         | INT         | Unique identifier for the branch |
| REGION            | VARCHAR2(50)| Region of the branch             |
| MANAGER_NAME      | VARCHAR2(100)| Name of the branch manager       |
| LAST_AUDIT_DATE   | DATE        | Last audit date                  |
| IS_ACTIVE         | CHAR(1)     | Active status indicator          |

### Target Data Model
#### BRANCH_SUMMARY_REPORT
| Column Name       | Data Type   | Description                      |
|-------------------|-------------|----------------------------------|
| BRANCH_ID         | INT         | Unique identifier for the branch |
| BRANCH_NAME       | STRING      | Name of the branch               |
| TOTAL_TRANSACTIONS| BIGINT      | Total number of transactions     |
| TOTAL_AMOUNT      | DOUBLE      | Total transaction amount         |
| REGION            | STRING      | Region of the branch             |
| LAST_AUDIT_DATE   | DATE        | Last audit date                  |

## Source-to-Target Mapping
| Source Column                  | Target Column                  | Transformation Rule              |
|--------------------------------|---------------------------------|----------------------------------|
| BRANCH_OPERATIONAL_DETAILS.REGION | BRANCH_SUMMARY_REPORT.REGION | Populate if IS_ACTIVE = 'Y'     |
| BRANCH_OPERATIONAL_DETAILS.LAST_AUDIT_DATE | BRANCH_SUMMARY_REPORT.LAST_AUDIT_DATE | Populate if IS_ACTIVE = 'Y' |

## Assumptions and Constraints
- The `BRANCH_OPERATIONAL_DETAILS` table is populated and maintained by the Oracle database.
- The `BRANCH_SUMMARY_REPORT` table is stored in Databricks Delta.
- Backward compatibility with older records must be maintained.
- Deployment requires a full reload of the `BRANCH_SUMMARY_REPORT` table.

## References
- JIRA Story: Extend BRANCH_SUMMARY_REPORT Logic to Integrate New Source Table
- Confluence Documentation: ETL Change - Integration of BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT
- Source DDL: BRANCH_OPERATIONAL_DETAILS
- Target DDL: BRANCH_SUMMARY_REPORT