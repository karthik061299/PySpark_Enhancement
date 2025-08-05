=============================================
Author: Ascendion AVA+
Date: <Leave it blank>
Description: Integration of BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT
=============================================

# Technical Specification for BRANCH_SUMMARY_REPORT Enhancement

## Introduction
This document outlines the technical specification for integrating the BRANCH_OPERATIONAL_DETAILS table into the BRANCH_SUMMARY_REPORT table as part of compliance and audit readiness enhancements.

## Code Changes
### Scala ETL Logic
- Perform LEFT JOIN between BRANCH_SUMMARY_REPORT and BRANCH_OPERATIONAL_DETAILS on BRANCH_ID.
- Apply conditional logic to populate REGION and LAST_AUDIT_DATE based on IS_ACTIVE = 'Y'.
- Ensure backward compatibility with existing records.

### Example SQL Transformation
```sql
SELECT
    bs.*,
    CASE 
        WHEN bod.IS_ACTIVE = 'Y' THEN bod.REGION
        ELSE NULL
    END AS REGION,
    CASE 
        WHEN bod.IS_ACTIVE = 'Y' THEN bod.LAST_AUDIT_DATE
        ELSE NULL
    END AS LAST_AUDIT_DATE
FROM
    BRANCH_SUMMARY_REPORT bs
LEFT JOIN
    BRANCH_OPERATIONAL_DETAILS bod
    ON bs.BRANCH_ID = bod.BRANCH_ID
```

## Data Model Updates
### Target Table: BRANCH_SUMMARY_REPORT
- Add new columns:
  - REGION (STRING)
  - LAST_AUDIT_DATE (DATE)

### ALTER TABLE Statement
```sql
ALTER TABLE BRANCH_SUMMARY_REPORT
  ADD COLUMN REGION STRING,
  ADD COLUMN LAST_AUDIT_DATE DATE;
```

## Source-to-Target Mapping
| Source Table                | Source Column      | Target Table           | Target Column      | Transformation Rule / Logic                                     |
|-----------------------------|-------------------|------------------------|-------------------|------------------------------------------------------------------|
| BRANCH_OPERATIONAL_DETAILS  | REGION            | BRANCH_SUMMARY_REPORT  | REGION            | If IS_ACTIVE = 'Y', set REGION; else NULL                       |
| BRANCH_OPERATIONAL_DETAILS  | LAST_AUDIT_DATE   | BRANCH_SUMMARY_REPORT  | LAST_AUDIT_DATE   | If IS_ACTIVE = 'Y', set LAST_AUDIT_DATE; else NULL              |

## Assumptions and Constraints
- Only active branches (IS_ACTIVE = 'Y') will have REGION and LAST_AUDIT_DATE populated.
- Backward compatibility is maintained for existing records.
- A full reload of BRANCH_SUMMARY_REPORT is required upon deployment.

## References
- JIRA Story: Extend BRANCH_SUMMARY_REPORT Logic to Integrate New Source Table
- Confluence Documentation: ETL Change - Integration of BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT

---
End of Document.