import sys
import os
import time
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

sys.path.append('/opt/airflow/source')

from utils.get_token import get_battle_net_access_token
from utils.get_league_data import league_data
from utils.get_ladder import fetch_legacy_ladder
from utils.get_match_history import fetch_match_history

CLIENT_ID = os.getenv('BLIZZARD_CLIENT_ID', 'COLOQUE_SEU_CLIENT_ID_AQUI')
CLIENT_SECRET = os.getenv('BLIZZARD_CLIENT_SECRET', 'COLOQUE_SEU_SECRET_AQUI')
BRONZE_PATH = '/opt/airflow/source/bronze'

def _get_token(**context):
    token = get_battle_net_access_token(CLIENT_ID, CLIENT_SECRET)
    if not token:
        raise ValueError("Falha ao obter o token da Blizzard!")
    return token

def _extract_diamond_league(**context):
    token = context['ti'].xcom_pull(task_ids='get_token')
    os.makedirs(BRONZE_PATH, exist_ok=True)
    df_league = league_data(season_id=66, queue_id=201, team_type=0, league_id=4, token=token)
    
    exec_date = context['logical_date'].strftime('%Y-%m-%d')
    file_path = f"{BRONZE_PATH}/diamond_league_{exec_date}.json"
    df_league.to_json(file_path, orient="records", lines=True)
    return df_league['ladder_id'].tolist()

def _extract_ladder_players(**context):
    token = context['ti'].xcom_pull(task_ids='get_token')
    ladder_ids = context['ti'].xcom_pull(task_ids='extract_league')
    all_ladders_data = []
    
    for ladder_id in ladder_ids:
        try:
            df_ladder = fetch_legacy_ladder(acesso=token, ladder_id=ladder_id)
            if not df_ladder.empty:
                df_ladder['source_ladder_id'] = ladder_id 
                all_ladders_data.append(df_ladder)
        except Exception as e:
            pass
            
    if all_ladders_data:
        exec_date = context['logical_date'].strftime('%Y-%m-%d')
        df_all_ladders = pd.concat(all_ladders_data, ignore_index=True)
        file_path = f"{BRONZE_PATH}/all_ladders_combined_{exec_date}.json"
        df_all_ladders.to_json(file_path, orient="records", lines=True)

def _extract_match_history(**context):
    token = context['ti'].xcom_pull(task_ids='get_token')
    exec_date = context['logical_date'].strftime('%Y-%m-%d')
    
    file_path = f"{BRONZE_PATH}/all_ladders_combined_{exec_date}.json"
    df_ladder = pd.read_json(file_path, orient="records", lines=True)
    
    all_players = df_ladder.to_dict('records')
    all_matches = []
    
    def fetch_player_data(row):
        try:
            p_id = row['character.id']
            rm_id = row['character.realm']
            rg_id = row['character.region']
            source_ladder = row['source_ladder_id']
            
            time.sleep(0.5) 
            df = fetch_match_history(token, rg_id, rm_id, p_id)
            if not df.empty:
               df['source_ladder_id'] = source_ladder 
            return df
        except Exception as e:
            return pd.DataFrame()

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fetch_player_data, p): p for p in all_players}
        for future in as_completed(futures):
            df_result = future.result()
            if not df_result.empty:
                all_matches.append(df_result)
            
    if all_matches:
        df_all_matches = pd.concat(all_matches, ignore_index=True)
        out_file = f"{BRONZE_PATH}/matches_all_diamonds_{exec_date}.json"
        df_all_matches.to_json(out_file, orient="records", lines=True)

default_args = {
    'owner': 'capitao_sc2',
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
}

with DAG('bronze_match_history_scraper',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False,
         tags=['starcraft', 'esports', 'data_lake']) as dag:

    get_token = PythonOperator(task_id='get_token', python_callable=_get_token)
    extract_league = PythonOperator(task_id='extract_league', python_callable=_extract_diamond_league)
    extract_ladder = PythonOperator(task_id='extract_ladder', python_callable=_extract_ladder_players)
    extract_matches = PythonOperator(task_id='extract_matches', python_callable=_extract_match_history)

    get_token >> extract_league >> extract_ladder >> extract_matches
