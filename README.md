# dbt Portfolio

A collection of dbt models built for an EdTech platform, covering program operations, attendance tracking, and client performance reporting.

> **Note:** All models in this repository have been anonymized. Company names, client names, internal project IDs, and personally identifiable references have been replaced with generic equivalents. The business logic, data modeling patterns, and dbt best practices are fully preserved.

---

## Tech Stack

- **dbt (Data Build Tool)** — transformation framework
- **BigQuery** — cloud data warehouse
- **Jinja2** — templating within SQL models
- **YAML** — schema definitions, contracts, and tests
- **Elementary** — observability and data quality monitoring

---

## Project Structure

```
dbt-portfolio/
├── models/
│   ├── staging/
│   │   └── attendance/
│   │       ├── stg_attendance_tracking_sheet.sql   ← ephemeral staging model
│   │       └── _stg_attendance.yml
│   └── gold/
│       ├── program_ops/
│       │   ├── gld_program_ops__attendance_tracking_sheet.sql  ← gold attendance model
│       │   └── _gld_program_ops.yml
│       └── client_mbr/
│           ├── gld_client_mbr__hist_mbr_snapshot.sql  ← incremental MBR snapshot
│           └── _gld_client_mbr.yml
└── docs/
    ├── attendance_README.md    ← attendance domain deep-dive
    ├── client_mbr_README.md    ← client MBR domain deep-dive
    └── tests_README.md         ← dbt testing patterns and conventions
```

---

## Models Overview

### Staging Layer

| Model | Materialization | Description |
|---|---|---|
| `stg_attendance_tracking_sheet` | ephemeral | Cleans and consolidates raw attendance data from Zoom API, handling overlapping session intervals, excused absences, and learner pathway exemptions |

### Gold Layer

| Model | Materialization | Description |
|---|---|---|
| `gld_program_ops__attendance_tracking_sheet` | table | Full learner × lesson attendance grid with week-level aggregation, waived/excused/exempt logic, and upcoming week scaffolding |
| `gld_client_mbr__hist_mbr_snapshot` | incremental | Monthly Business Review (MBR) historical snapshot with incremental merge strategy, EU roll-up aggregation, and certification tracking |

---

## Key Patterns Demonstrated

- **Incremental models** with `merge` strategy, `incremental_predicates`, and full-refresh protection
- **Ephemeral CTEs** for intermediate transformation steps without materializing intermediate tables
- **Interval merging logic** — consolidating overlapping Zoom session timestamps per participant
- **User-lesson grid generation** — cartesian join pattern to build attendance scaffolds
- **Macro-based labeling and aliasing** — `get_table_name()`, `get_labels()` for consistent metadata
- **dbt contracts** with enforced column types and primary key constraints
- **Generic and singular tests** with Elementary integration for data quality observability
- **QUALIFY clause** for efficient deduplication within window functions
- **Dynamic UNNEST + GENERATE_DATE_ARRAY** for date range expansion (waived attendance)
- **EU roll-up aggregation** — unioning individual country rows with a derived regional aggregate

---

## Documentation

See the `/docs` folder for domain-specific documentation:
- **Attendance domain**: data sources, known limitations, layer-by-layer ERD
- **Client MBR domain**: model dependencies (Mermaid diagrams), business context, example queries  
- **Testing conventions**: generic vs singular tests, Elementary setup, compliance test patterns
