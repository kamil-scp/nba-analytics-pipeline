from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="dbt_bigquery",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    run_seed = BashOperator(
        task_id="seeds_dbt",
        bash_command="cd /opt/airflow/dbt/nba_project_dbt && dbt seed",
    )

    run_snapshot = BashOperator(
        task_id="snapshots_dbt",
        bash_command="cd /opt/airflow/dbt/nba_project_dbt && dbt snapshot",
    )

    run_model = BashOperator(
        task_id="models_dbt",
        bash_command="cd /opt/airflow/dbt/nba_project_dbt && dbt run",
    )

    run_test = BashOperator(
        task_id="tests_dbt",
        bash_command="cd /opt/airflow/dbt/nba_project_dbt && dbt test",
    )

    run_seed >> run_snapshot >> run_model >> run_test
