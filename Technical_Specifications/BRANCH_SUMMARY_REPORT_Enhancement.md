# Technical Specification for BRANCH_SUMMARY_REPORT Enhancement

-=============================================
Author: Ascendion AVA+
Date: 
Description: Integration of BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT
-=============================================

## Introduction

### Objective
To integrate the BRANCH_OPERATIONAL_DETAILS table into the BRANCH_SUMMARY_REPORT table to enhance compliance and audit readiness.

### Scope
This enhancement involves updating the ETL logic, data models, and source-to-target mapping.

## Code Changes

### Impacted Areas
- **Scala ETL Logic**: Enhance the logic to join BRANCH_OPERATIONAL_DETAILS using BRANCH_ID.
- **Conditional Population**: Populate REGION and LAST_AUDIT_DATE columns based on IS_ACTIVE = 'Y'.

### Pseudocode
```scala
// Scala pseudocode for ETL logic
val branchOperationalDetails = spark.read.format("jdbc")
  .option("url", jdbcUrl)
  .option("dbtable", "BRANCH_OPERATIONAL_DETAILS")
  .load()

val branchSummaryReport = branchOperationalDetails
  .filter("IS_ACTIVE = 'Y'")
  .join(branchSummaryReport, "BRANCH_ID")
  .select("BRANCH_ID", "REGION", "LAST_AUDIT_DATE")
```

## Data Model Updates

### Source Data Model
- **BRANCH_OPERATIONAL_DETAILS**
  - REGION: VARCHAR2(50)
  - LAST_AUDIT_DATE: DATE

### Target Data Model
- **BRANCH_SUMMARY_REPORT**
  - Add REGION column
  - Add LAST_AUDIT_DATE column

## Source-to-Target Mapping

| Source Column                     | Target Column                     | Transformation Rule                  |
|-----------------------------------|-----------------------------------|--------------------------------------|
| BRANCH_OPERATIONAL_DETAILS.REGION | BRANCH_SUMMARY_REPORT.REGION      | Populate directly                    |
| BRANCH_OPERATIONAL_DETAILS.LAST_AUDIT_DATE | BRANCH_SUMMARY_REPORT.LAST_AUDIT_DATE | Populate if IS_ACTIVE = 'Y'         |

## Assumptions and Constraints

- **Backward Compatibility**: Older records in BRANCH_SUMMARY_REPORT must remain unaffected.
- **Full Reload**: A full reload of BRANCH_SUMMARY_REPORT is required.

## References

- JIRA Story: Extend BRANCH_SUMMARY_REPORT Logic to Integrate New Source Table
- Confluence Documentation: ETL Change - Integration of BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT