# ITSM Data Pipeline Project

This repository contains all artifacts to ingest, transform, and visualize an ITSM (ServiceNow) ticket dataset using Postgres, DBT, Apache Airflow, and Apache Superset.

## Project structure
- `data/clean_ticket_snapshot.csv` - cleaned CSV snapshot (from provided dataset)
- `airflow/dags/dbt_etl_dag.py` - Airflow DAG to ingest CSV into Postgres, run DBT, and validate.
- `dbt/` - DBT project with models, SQL transformations and schema tests.
- `superset/superset_dashboard_export.json` - Superset dashboard export (template).
- `scripts/load_to_postgres.py` - Python script to load CSV into Postgres using psycopg2.
- `requirements.txt` - Python dependencies for local setup.
- `README.md` - This file.

## How to run locally (high-level)
1. Install Postgres and create a database (e.g., `itsm_db`). Create a user with password.
2. Install Python dependencies: `pip install -r requirements.txt`.
3. Load CSV into Postgres:
   - `python scripts/load_to_postgres.py --csv data/clean_ticket_snapshot.csv --table tickets_raw --db-name itsm_db --db-user <user> --db-pass <pass> --db-host <host>`
   - or use `psql` with COPY command.
4. Configure DBT profiles in `dbt/profiles.yml` (example provided in README section of dbt folder).
5. Run DBT:
   - `cd dbt && dbt deps && dbt seed && dbt run && dbt test`
6. Start Airflow (if desired) and ensure DAG `dbt_etl_dag` is available. The DAG will run ingestion + dbt commands.
7. Connect Superset to the Postgres DB and import `superset/superset_dashboard_export.json` to create the dashboard.