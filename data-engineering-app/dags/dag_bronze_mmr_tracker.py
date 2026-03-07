import sys
import os
import time
import json
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

# Tenta carregar o arquivo .env se estiver rodando localmente (fora do docker)
load_dotenv()

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Adiciona a pasta source ao path do Python interno do Airflow (graças ao volume que mapeamos!)
sys.path.append('/opt/airflow/source')

from utils.get_token import get_battle_net_access_token
from utils.get_league_data import get_league_data_raw
from utils.get_ladder import fetch_ladder_modern_raw, fetch_ladder_legacy_raw
from utils.get_match_history import fetch_match_history_raw

# Credenciais vindas de Váriaveis de Ambiente (Para não expor no GitHub)
CLIENT_ID = os.getenv('BLIZZARD_CLIENT_ID', 'COLOQUE_SEU_CLIENT_ID_AQUI')
CLIENT_SECRET = os.getenv('BLIZZARD_CLIENT_SECRET', 'COLOQUE_SEU_SECRET_AQUI')

# Nossa pasta do Data Lake local (Será criada dentro de rede-nydus/source/bronze)
BRONZE_PATH = '/opt/airflow/source/bronze'

def _get_token(**context):
    """ Task 1: Busca o token da API e compartilha com as próximas tasks via XCom """
    token = get_battle_net_access_token(CLIENT_ID, CLIENT_SECRET)
    if not token:
        raise ValueError("Falha ao obter o token da Blizzard!")
    return token 

def _extract_diamond_league(**context):
    """ Task 2: Puxa todos os jogadores da liga de diamante e salva na tabaela da camada Bronze (Formato RAW puro) """
    token = context['ti'].xcom_pull(task_ids='get_token')
    
    os.makedirs(BRONZE_PATH, exist_ok=True)
    
    print("Baixando dados RAW da Liga...")
    raw_data = get_league_data_raw(season_id=66, queue_id=201, team_type=0, league_id=4, token=token)
    
    # Obtém a data de execução cronometrada pelo airflow
    exec_date = context['logical_date'].strftime('%Y-%m-%d_%H%M')
    file_path = f"{BRONZE_PATH}/diamond_league_raw_{exec_date}.json"
    
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(raw_data, f)
    print(f"Sucesso! Dados brutos da liga salvos em: {file_path}")
    
    # Retirar IDs das ladders puramente do dicionário pra avançar pra próxima task
    all_ladder_ids = []
    for tier in raw_data.get('tier', []):
        for division in tier.get('division', []):
            all_ladder_ids.append(division['ladder_id'])
            
    return all_ladder_ids

def _extract_modern_ladders(**context):
    """ Task 3a: Puxa o RAW JSON Exclusivo da API de Data (Moderna) """
    token = context['ti'].xcom_pull(task_ids='get_token')
    ladder_ids = context['ti'].xcom_pull(task_ids='extract_league')
    
    modern_raw_data = []
    
    for ladder_id in ladder_ids:
        print(f"Baixando RAW JSON Modern da ladder_id: {ladder_id}")
        try:
            data = fetch_ladder_modern_raw(acesso=token, ladder_id=ladder_id)
            if data:
                modern_raw_data.append({
                    "ladder_id": ladder_id,
                    "data": data
                })
        except Exception as e:
            print(f"Erro no ladder modern {ladder_id}: {e}")
            
    if modern_raw_data:
        exec_date = context['logical_date'].strftime('%Y-%m-%d_%H%M')
        file_path = f"{BRONZE_PATH}/modern_ladders_raw_{exec_date}.json"
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(modern_raw_data, f)
        print(f"Sucesso! {len(modern_raw_data)} Respostas Modern RAW salvas em: {file_path}")
    else:
        print("Nenhuma ladder modern processada.")

def _extract_legacy_ladders(**context):
    """ Task 3b: Puxa o RAW JSON Exclusivo da API Legacy (Antiga - Clãs) """
    token = context['ti'].xcom_pull(task_ids='get_token')
    ladder_ids = context['ti'].xcom_pull(task_ids='extract_league')
    
    legacy_raw_data = []
    
    for ladder_id in ladder_ids:
        print(f"Baixando RAW JSON Legacy da ladder_id: {ladder_id}")
        try:
            data = fetch_ladder_legacy_raw(acesso=token, ladder_id=ladder_id)
            if data:
                legacy_raw_data.append({
                    "ladder_id": ladder_id,
                    "data": data
                })
        except Exception as e:
            print(f"Erro no ladder legacy {ladder_id}: {e}")
            
    if legacy_raw_data:
        exec_date = context['logical_date'].strftime('%Y-%m-%d_%H%M')
        file_path = f"{BRONZE_PATH}/legacy_ladders_raw_{exec_date}.json"
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(legacy_raw_data, f)
        print(f"Sucesso! {len(legacy_raw_data)} Respostas Legacy RAW salvas em: {file_path}")
    else:
        print("Nenhuma ladder legacy processada.")


# ====================
# DEFINIÇÃO DO FLUXO DO AIRFLOW (DAG)
# ====================
default_args = {
    'owner': 'capitao_sc2',
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
}

with DAG('bronze_mmr_snapshot_tracker',
         default_args=default_args,
         schedule_interval='*/10 * * * *', # Roda a CADA 10 MINUTOS
         catchup=False,
         tags=['starcraft', 'esports', 'data_lake', 'mmr_tracker']) as dag:

    get_token = PythonOperator(
        task_id='get_token',
        python_callable=_get_token
    )

    extract_league = PythonOperator(
        task_id='extract_league',
        python_callable=_extract_diamond_league
    )

    extract_modern_ladders = PythonOperator(
        task_id='extract_modern_ladders',
        python_callable=_extract_modern_ladders
    )

    extract_legacy_ladders = PythonOperator(
        task_id='extract_legacy_ladders',
        python_callable=_extract_legacy_ladders
    )

    # Orquestração: Extrai a liga, e depois dispara as duas extrações modern e legacy em paralelo!
    get_token >> extract_league >> [extract_modern_ladders, extract_legacy_ladders]