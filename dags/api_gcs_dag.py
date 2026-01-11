from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sdk import Variable
from airflow.hooks.base import BaseHook

from google.cloud import bigquery, storage
from google.oauth2 import service_account

from datetime import datetime
import pandas as pd
import os
import io
from datetime import timedelta

from nba_api.stats.endpoints import leaguegamefinder, BoxScoreTraditionalV2
from nba_api.stats.static import players, teams


# ================================================================
# 1. Getting GCP Credentials
# ================================================================
def get_gcp_params(**kwargs):
    params = Variable.get("gcp_dict", deserialize_json=True)
    bq_conn = BaseHook.get_connection("cred_json_gcp")
    kwargs["ti"].xcom_push(key="gcp_params", value=params)


# ================================================================
# 2. Exporting data from NBA API
# ================================================================
def export_games(**kwargs):
    season = kwargs["season"]
    gamefinder = leaguegamefinder.LeagueGameFinder(
        season_type_nullable="Regular Season",
        season_nullable=season
    )
    games = gamefinder.get_data_frames()[0]
    games["GAME_DATE"] = pd.to_datetime(games["GAME_DATE"])
    kwargs["ti"].xcom_push("games_df", games.to_json(orient="records"))


# ================================================================
# 3. Transformations
# ================================================================
def transform_games(**kwargs):
    ti = kwargs["ti"]
    games_json = ti.xcom_pull(key="games_df", task_ids="export_games")
    games_df = pd.read_json(games_json)

    games_df["IF_HOME_GAME"] = ["HG" if "vs." in m else "AG" for m in games_df["MATCHUP"]]

    games_grouped = (
        games_df.set_index(["GAME_ID", "IF_HOME_GAME"])
        .unstack()
        .reset_index()
    )
    games_grouped.columns = [
        col[0] if col[1] == "" else f"{col[0]}_{col[1]}" for col in games_grouped.columns
    ]

    ti.xcom_push(key="games_grouped", value=games_grouped.to_json(orient="records"))


# ================================================================
# 4. Export players statistics
# ================================================================
def export_player_stats(**kwargs):
    ti = kwargs["ti"]

    games_grouped_json = ti.xcom_pull(key="games_grouped", task_ids="transform_games")
    games_grouped_df = pd.read_json(games_grouped_json)

    games_list = [int(gid) for gid in games_grouped_df["GAME_ID"].unique()][:3]

    fact_df = pd.DataFrame()
    for gid in games_list:
        box = BoxScoreTraditionalV2(game_id=f'00{gid}')
        fact_df = pd.concat([fact_df, box.get_data_frames()[0]], ignore_index=True)

    ti.xcom_push("players_fact", fact_df.to_json(orient="records"))


# ================================================================
# 5. Upload parquet do GCS
# ================================================================
def upload_to_gcs(**kwargs):
    ti = kwargs["ti"]

    project_dict = Variable.get("gcp_dict", deserialize_json=True)
    bucket_name = project_dict["bucket_name"]
    project_id = project_dict["project_id"]

    gcs_conn = BaseHook.get_connection("cred_json_gcp")
    gcs_json = gcs_conn.extra_dejson
    creds = service_account.Credentials.from_service_account_info(gcs_json)

    client = storage.Client(credentials=creds, project=project_id)
    bucket = client.bucket(bucket_name)

    # Pobranie danych
    teams_df = pd.DataFrame(teams.get_teams())
    players_df = pd.DataFrame(players.get_players())

    games_grouped = pd.read_json(ti.xcom_pull(key="games_grouped", task_ids="transform_games"))
    players_fact = pd.read_json(ti.xcom_pull(key="players_fact", task_ids="export_player_stats"))

    dfs = {
        "teams": teams_df,
        "players": players_df,
        "games": games_grouped,
        "players_fact": players_fact
    }

    exceptions_columns_types = {
        "games": ["GAME_ID", "SEASON_ID"],
        "players_fact": ["GAME_ID"]
    }

    for name, df_data in dfs.items():            
        if name in exceptions_columns_types:
            #print(df_data.columns())
            for col in exceptions_columns_types[name]:
                if col in df_data.columns:
                    df_data[col] = df_data[col].astype(str)
        blob = bucket.blob(f"{name}.parquet")
        buffer = io.BytesIO()
        df_data.to_parquet(buffer, index=False)
        buffer.seek(0)
        blob.upload_from_file(buffer)

    print("Uploaded all parquet files.")


# ================================================================
# 6. Create External Tables in BigQuery
# ================================================================
def create_bq_external(**kwargs):
    ti = kwargs["ti"]

    project_dict = Variable.get("gcp_dict", deserialize_json=True)
    bucket_name = project_dict["bucket_name"]
    project_id = project_dict["project_id"]
    dataset = project_dict["dataset_name"]

    bq_conn = BaseHook.get_connection("bq_cred_json")
    bq_json = bq_conn.extra_dejson
    credentials_bq = service_account.Credentials.from_service_account_info(bq_json)

    gcs_conn = BaseHook.get_connection("cred_json_gcp")
    gcs_json = gcs_conn.extra_dejson
    credentials_gcs = service_account.Credentials.from_service_account_info(gcs_json)

    client_bq = bigquery.Client(credentials=credentials_bq, project=project_id)
    client_gcs = storage.Client(credentials=credentials_gcs, project=project_id)
    bucket = client_gcs.bucket(bucket_name)

    for blob in bucket.list_blobs():
        if not blob.name.endswith(".parquet"):
            continue

        table_name = os.path.splitext(os.path.basename(blob.name))[0]
        table_id = f"{project_id}.{dataset}.{table_name}"

        external = bigquery.ExternalConfig("PARQUET")
        external.source_uris = [f"gs://{bucket_name}/{blob.name}"]
        external.autodetect = True

        table = bigquery.Table(table_id)
        table.external_data_configuration = external

        client_bq.create_table(table, exists_ok=True)
        print(f"✅ Created external table: {table_id}")


# ================================================================
# DAG
# ================================================================
with DAG(
    "nba_etl_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    dagrun_timeout=timedelta(minutes=5), 
    max_active_runs=3                    
) as dag:

    get_gcp_params_task = PythonOperator(
        task_id="get_gcp_params",
        python_callable=get_gcp_params
    )

    export_games_task = PythonOperator(
        task_id="export_games",
        python_callable=export_games,
        op_kwargs={"season": "2022-23"}
    )

    transform_games_task = PythonOperator(
        task_id="transform_games",
        python_callable=transform_games
    )

    export_player_stats_task = PythonOperator(
        task_id="export_player_stats",
        python_callable=export_player_stats
    )

    upload_to_gcs_task = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_to_gcs
    )

    create_bq_task = PythonOperator(
        task_id="create_bq_external",
        python_callable=create_bq_external
    )

    get_gcp_params_task >> export_games_task >> transform_games_task \
        >> export_player_stats_task >> upload_to_gcs_task >> create_bq_task
