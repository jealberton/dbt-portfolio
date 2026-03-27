# Client MBR Models <!-- omit from toc -->
- [Context](#context)
- [Models Overview](#models-overview)
  - [Dataset: `gld_client_mbr`](#dataset-gld_client_mbr)
  - [`client_mbr_summary`](#client_mbr_summary)
  - [`ccsp_learners`](#ccsp_learners)
  - [`certification_updates`](#certification_updates)
- [Example Queries](#example-queries)
  - [1. Check ongoing programs per country](#1-check-ongoing-programs-per-country)
  - [2. Check Historical Snapshot Data](#2-check-historical-snapshot-data)

## Context
This is the company's most significant client, with a wide variety of programs. Monthly performance tracking is conducted for these programs, monitoring key metrics such as retention rate, placement rate, NPS, cost per placement, and others. The underlying data comes from placement_platform, and the Data Analytics team prepares the necessary visualizations on demand for the business team and stakeholders who will deliver the monthly presentation. The goal of this dataset is to ensure standardization, reliability, and traceability of the data shared with stakeholders, as well as to uphold current business rules.

## Models Overview
### Dataset: `gld_client_mbr`
### `client_mbr_summary`
- **Dependencies**:
  ```mermaid
        graph LR
          client_mbr_summary["client_mbr_summary"]
          gld_job_outcomes__outcomes_master_table --> client_mbr_summary
          stg_client_mbr__program_nps --> client_mbr_summary
          stg_client_mbr__ccsp_aggregated --> client_mbr_summary  
  ```
### `ccsp_learners`
- **Dependencies**:
  ```mermaid
        graph LR
          ccsp_learners["ccsp_learners"]
          slv_ccsp_fellow_program_fact --> ccsp_learners
          slv_ccsp_program_dim --> ccsp_learners
          slv_ccsp_fellow_dim --> ccsp_learners
          slv_grading__grades_fact --> ccsp_learners
          brz_client_mbr__ccsp_program_names-mapping_sheet --> ccsp_learners
          slv_program_service__programs --> ccsp_learners
  ```

### `certification_updates`
- **Dependencies**:
  ```mermaid
      graph LR
        certification_updates["certification_updates"]
        gld_client_mbr__client_mbr_summary --> certification_updates
        gld_client_mbr__ccsp_learners --> certification_updates
          ```

### `hist_mbr_snapshot`
- **Dependencies**:
  ```mermaid
      graph LR
        hist_mbr_snapshot["hist_mbr_snapshot"]
        gld_client_mbr__client_mbr_summary --> hist_mbr_snapshot
        brz_client_mbr__backfill_sheet --> hist_mbr_snapshot
        slv_airtable_programs__programs_dim --> hist_mbr_snapshot
        gld_client_mbr__certification_updates --> hist_mbr_snapshot
  ```



## Example Queries

### 1. Check ongoing programs per country
Retrieve ongoing programs for the current month:
```sql
SELECT program_code, country  
FROM `proj-nprd-002.gold_client_mbr.client_mbr_summary`  
WHERE months_since_completion = 'Ongoing';
```

### 2. Check Historical Snapshot Data
Review the ordered history of snapshots:
```sql
SELECT *  
FROM `proj-prod-002.gold_client_mbr.hist_mbr_snapshot`  
ORDER BY  
    snapshot_date DESC,  
    CASE  
        WHEN months_since_completion = 'Ongoing' THEN 999  
        ELSE SAFE_CAST(months_since_completion AS INT64)  
    END,  
    program_code ASC;

```