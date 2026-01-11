from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator, PythonOperator
from google.cloud import bigquery
from airflow.hooks.base import BaseHook
from airflow.sdk import Variable
from google.oauth2 import service_account
import datetime
import pandas as pd
import logging
import tempfile
import os

default_args = {
    'retries': 3,
    'retry_delay': 10
}

# -----------------------------
# Task 1: Web scraping (Virtualenv)
# -----------------------------
def webscraping_virtualenv(start_season: int, final_season: int):
    """
    Scrapes NBA team totals for seasons from start_season to final_season.
    Runs in an isolated Python virtual environment.
    Returns path to a temporary CSV file.
    """
    import requests
    import pandas as pd
    from bs4 import BeautifulSoup
    import tempfile
    import logging

    logging.info(f"Scraping seasons {start_season} to {final_season}")
    season_stats_url_base = 'https://www.basketball-reference.com/leagues/NBA_'
    seasons_table = pd.DataFrame()

    def get_season_table(season_year_url):
        """Fetches a single season table and returns as a DataFrame."""
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(season_year_url, headers=headers, timeout=10)
        if response.status_code != 200:
            raise Exception(f"Failed to fetch {season_year_url}, status: {response.status_code}")
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find("table", id="totals-team")
        if table is None:
            raise Exception(f"No table found for {season_year_url}")
        return pd.read_html(str(table))[0]

    # Loop over each season and append to combined DataFrame
    for season in range(start_season, final_season + 1):
        url = f"{season_stats_url_base}{season}.html"
        df = get_season_table(url)
        df['season_year'] = season
        seasons_table = pd.concat([seasons_table, df], ignore_index=True)

    # Clean column names: convert '2P' -> 'TwoP', '3P' -> 'ThreeP', etc.
    def clean_col(name):
        mapping = {'2': 'Two', '3': 'Three'}
        if name[0].isdigit():
            return mapping.get(name[0], name[0]) + name[1:]
        return name

    seasons_table.columns = [clean_col(c) for c in seasons_table.columns]

    # Save combined DataFrame to a temporary CSV
    tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    seasons_table.to_csv(tmp_file.name, index=False)
    logging.info(f"Data saved to {tmp_file.name}")

    # Return the file path; PythonVirtualenvOperator stores it in XCom automatically
    return tmp_file.name

# -----------------------------
# Task 2: Insert data into BigQuery
# -----------------------------
def df_insert_bq(table_name: str, **kwargs):
    """
    Loads CSV from XCom into a BigQuery table.
    """
    ti = kwargs['ti']
    # Pull CSV file path from XCom pushed by virtualenv task
    csv_file_path = ti.xcom_pull(task_ids='webscraping_sites')

    logging.info(f"Inserting {csv_file_path} into BQ table {table_name}")
    df = pd.read_csv(csv_file_path)

    # Connect to BigQuery using Airflow connection
    conn = BaseHook.get_connection("bq_cred_json")
    extra = conn.extra_dejson
    credentials = service_account.Credentials.from_service_account_info(extra)
    project_id = extra["project_id"]
    dataset = Variable.get("dataset_name")
    table_name = f'{project_id}.{dataset}.{table_name}'

    client = bigquery.Client(project=project_id, credentials=credentials)
    job = client.load_table_from_dataframe(
        df,
        table_name,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    )
    job.result()
    logging.info(f"{table_name} has been inserted successfully!")

    # Remove temporary CSV file after upload
    os.remove(csv_file_path)

# -----------------------------
# DAG definition
# -----------------------------
with DAG(
    dag_id='webscraping_prod_virtualenv',
    start_date=datetime.datetime(2025, 11, 15),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=['prod', 'nba']
) as dag:

    # Task: Web scraping in isolated virtualenv
    scrape_data = PythonVirtualenvOperator(
        task_id='webscraping_sites',
        python_callable=webscraping_virtualenv,
        op_args=[2023, 2025],
        # Install these packages in the isolated environment
        requirements=["pandas", "requests", "beautifulsoup4", "lxml"],
        system_site_packages=False,
        python_version='3.11'
    )

    # Task: Insert scraped CSV into BigQuery
    insert_data = PythonOperator(
        task_id='df_insert_bq',
        python_callable=df_insert_bq,
        op_args=['seasons_stats_tab']  # BigQuery table name
    )

    # Define task dependencies
    scrape_data >> insert_data
