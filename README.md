# Job Market Intelligence Platform (Python + PostgreSQL + Power BI)

End-to-end analytics engineering project that ingests job postings, normalizes data across sources, loads a star-schema warehouse in PostgreSQL, enriches records (skills/seniority/job type), publishes metrics as SQL views, and serves an executive Power BI dashboard. Runs daily via cron with observability and incremental ingestion state tracking.

## Key features
- Medallion-style pipeline (Raw → Normalized → Canonical → Warehouse → Metrics)
- Star schema warehouse + skill bridge table
- Incremental loading via `warehouse.ingestion_state`
- Observability via `warehouse.pipeline_runs`
- Metrics views in `metrics` schema
- Power BI executive dashboard (4 pages)
- Daily cron automation + logging

## Docs
See `/docs`:
- Architecture: docs/ARCHITECTURE.md
- Data flow: docs/DATA_FLOW.md
- Warehouse schema: docs/WAREHOUSE_SCHEMA.md
- Operations: docs/OPERATIONS.md