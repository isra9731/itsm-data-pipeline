from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'itsm_automation',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='dbt_etl_dag',
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',  # runs once every 24 hours
    catchup=False,
    max_active_runs=1
) as dag:

    ingest = BashOperator(
        task_id='ingest_csv_to_postgres',
        bash_command='python /opt/airflow/scripts/load_to_postgres.py --csv /opt/airflow/data/clean_ticket_snapshot.csv --table tickets_raw --db-name itsm_db --db-user {{ var.value.postgres_user }} --db-pass {{ var.value.postgres_pass }} --db-host {{ var.value.postgres_host }}'
    )

    run_dbt = BashOperator(
        task_id='run_dbt_models',
        bash_command='cd /opt/airflow/dbt && dbt deps && dbt seed && dbt run --profiles-dir . && dbt test --profiles-dir .'
    )

    validate = BashOperator(
        task_id='validate_dbt_run',
        bash_command='[ -f /opt/airflow/dbt/target/run_results.json ] && echo "dbt run results exists" || (echo "dbt results missing" && exit 1)'
    )

    ingest >> run_dbt >> validate