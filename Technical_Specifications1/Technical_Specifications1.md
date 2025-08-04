=============================================
Author: Ascendion AVA+
Date: <Leave it blank>
Description: Technical specification for integrating BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT
=============================================

# Technical Specification for BRANCH_OPERATIONAL_DETAILS Integration

## Introduction
This document outlines the technical specifications for integrating the BRANCH_OPERATIONAL_DETAILS table into the BRANCH_SUMMARY_REPORT table. The integration aims to enhance compliance and audit readiness by incorporating branch-level operational metadata.

## Code Changes
### Schema Update
- Alter BRANCH_SUMMARY_REPORT to add:
  - REGION STRING
  - LAST_AUDIT_DATE DATE

### ETL Logic
```scala
val branchOperationalDF = spark.read... // Load BRANCH_OPERATIONAL_DETAILS
val branchSummaryDF = spark.read... // Load BRANCH_SUMMARY_REPORT (existing logic)

val branchOperationalActiveDF = branchOperationalDF
  .filter($"IS_ACTIVE" === "Y")
  .select($"BRANCH_ID", $"REGION", $"LAST_AUDIT_DATE")

val updatedBranchSummaryDF = branchSummaryDF
  .join(branchOperationalActiveDF, Seq("BRANCH_ID"), "left")
  .withColumn("REGION", $"REGION")
  .withColumn("LAST_AUDIT_DATE", $"LAST_AUDIT_DATE")
```

### Backward Compatibility
- Ensure ETL logic does not overwrite existing records with NULLs for REGION and LAST_AUDIT_DATE unless there is no active branch record.

## Data Model Updates
### Source Table: BRANCH_OPERATIONAL_DETAILS
| Column Name      | Data Type      | Description                       |
|------------------|---------------|-----------------------------------|
| BRANCH_ID        | INT           | Primary key                       |
| REGION           | VARCHAR2(50)  | Branch region                     |
| MANAGER_NAME     | VARCHAR2(100) | Branch manager name               |
| LAST_AUDIT_DATE  | DATE          | Last audit date                   |
| IS_ACTIVE        | CHAR(1)       | 'Y' if branch is active, else 'N' |

### Target Table: BRANCH_SUMMARY_REPORT (Post-Enhancement)
| Column Name        | Data Type      | Description                        |
|--------------------|---------------|------------------------------------|
| BRANCH_ID          | INT           | Primary key                        |
| BRANCH_NAME        | STRING        | Branch name                        |
| TOTAL_TRANSACTIONS | BIGINT        | Total number of transactions       |
| TOTAL_AMOUNT       | DOUBLE        | Total transaction amount           |
| REGION             | STRING        | Branch region (from source)        |
| LAST_AUDIT_DATE    | DATE          | Last audit date (from source)      |

## Source-to-Target Mapping
| Target Column      | Source Table/Column                | Transformation Rule / Logic                                                                 |
|--------------------|------------------------------------|--------------------------------------------------------------------------------------------|
| BRANCH_ID          | Existing                           | No change                                                                                   |
| BRANCH_NAME        | Existing                           | No change                                                                                   |
| TOTAL_TRANSACTIONS | Existing                           | No change                                                                                   |
| TOTAL_AMOUNT       | Existing                           | No change                                                                                   |
| REGION             | BRANCH_OPERATIONAL_DETAILS.REGION  | If IS_ACTIVE = 'Y' for the branch, set REGION = REGION; else set REGION = NULL              |
| LAST_AUDIT_DATE    | BRANCH_OPERATIONAL_DETAILS.LAST_AUDIT_DATE | If IS_ACTIVE = 'Y' for the branch, set LAST_AUDIT_DATE = LAST_AUDIT_DATE; else set LAST_AUDIT_DATE = NULL |

## Assumptions and Constraints
- Only active branches (`IS_ACTIVE = 'Y'`) contribute REGION and LAST_AUDIT_DATE.
- The integration must not impact existing columns or logic for inactive branches.
- The solution must be audit-ready and maintainable, with clear transformation logic.

## References
- JIRA Story: "Extend BRANCH_SUMMARY_REPORT Logic to Integrate New Source Table"
- Confluence: [See confluence_content.txt for additional business context]
- Source DDL: branch_operational_details.sql
- Target DDL: target_ddl.sql

## Example Mapping
| BRANCH_ID | Existing Columns... | REGION (from source) | LAST_AUDIT_DATE (from source) |
|-----------|--------------------|----------------------|-------------------------------|
| 101       | ...                | 'NORTH'              | 2023-12-01                    |
| 102       | ...                | NULL                 | NULL                          |

## Appendix: DDL Changes
```sql
ALTER TABLE BRANCH_SUMMARY_REPORT
  ADD COLUMN REGION STRING,
  ADD COLUMN LAST_AUDIT_DATE DATE;
```

## Summary
This mapping ensures that the BRANCH_SUMMARY_REPORT table is enhanced with operational metadata from BRANCH_OPERATIONAL_DETAILS, with clear transformation rules and auditability, as required by the business.