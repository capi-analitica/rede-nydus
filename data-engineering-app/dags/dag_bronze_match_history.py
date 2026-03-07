import sys
import os
import time
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

# Tenta carregar o arquivo .env se estiver rodando localmente (fora do docker)
load_dotenv()

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

sys.path.append('/opt/airflow/source')

from utils.get_token import get_battle_net_access_token
from utils.get_league_data import get_league_data_raw
from utils.get_ladder import fetch_ladder_legacy_raw
from utils.get_match_history import fetch_match_history_raw

CLIENT_ID = os.getenv('BLIZZARD_CLIENT_ID', 'COLOQUE_SEU_CLIENT_ID_AQUI')
CLIENT_SECRET = os.getenv('BLIZZARD_CLIENT_SECRET', 'COLOQUE_SEU_SECRET_AQUI')
BRONZE_PATH = '/opt/airflow/source/bronze'

def _get_token(**context):
    token = get_battle_net_access_token(CLIENT_ID, CLIENT_SECRET)
    if not token:
        raise ValueError("Falha ao obter o token da Blizzard!")
    return token

def _extract_diamond_league(**context):
    """
    Camada Bronze - Extrai League (sem pandas)
    """
    token = context['ti'].xcom_pull(task_ids='get_token')
    os.makedirs(BRONZE_PATH, exist_ok=True)
    
    # Busca crua
    raw_data = get_league_data_raw(season_id=66, queue_id=201, team_type=0, league_id=4, token=token)
    
    exec_date = context['logical_date'].strftime('%Y-%m-%d_%H%M')
    file_path = f"{BRONZE_PATH}/diamond_league_history_{exec_date}.json"
    
    import json
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(raw_data, f, ensure_ascii=False)
        
    # Extrair os ladder_ids da string JSON bruta (sem pandas)
    ladder_ids = []
    for tier in raw_data.get('tier', []):
        for div in tier.get('division', []):
            ladder_ids.append(div.get('ladder_id'))
            
    return ladder_ids

def _extract_ladder_players(**context):
    """
    Camada Bronze - Extrai Ladders dos jogadores
    """
    token = context['ti'].xcom_pull(task_ids='get_token')
    ladder_ids = context['ti'].xcom_pull(task_ids='extract_league')
    
    all_ladders_raw = []
    for ladder_id in ladder_ids[:5]: # Simplificado pra 5 pois limitaria muito a runtime pra fins de teste! Remover dps ou manter num range maior
        try:
            dados_brutos = fetch_ladder_legacy_raw(acesso=token, ladder_id=ladder_id)
            if dados_brutos:
                all_ladders_raw.append(dados_brutos)
        except Exception:
            pass
            
    if all_ladders_raw:
        exec_date = context['logical_date'].strftime('%Y-%m-%d_%H%M')
        file_path = f"{BRONZE_PATH}/all_ladders_combined_history_{exec_date}.json"
        import json
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(all_ladders_raw, f, ensure_ascii=False)

def _extract_match_history(**context):
    """
    Camada Bronze - Extrai History dos jogadores (sem pandas)
    """
    import json
    token = context['ti'].xcom_pull(task_ids='get_token')
    exec_date = context['logical_date'].strftime('%Y-%m-%d_%H%M')
    
    file_path = f"{BRONZE_PATH}/all_ladders_combined_history_{exec_date}.json"
    
    if not os.path.exists(file_path):
        print("Arquivo de ladders da execução atual não encontrado.")
        return

    with open(file_path, 'r', encoding='utf-8') as f:
        ladders_data = json.load(f)
    
    all_players = []
    # Navega pelo json bruto da Blizzard
    for ladder in ladders_data:
        members = ladder.get('ladderMembers', [])
        for member in members:
            character = member.get('character', {})
            if character:
                all_players.append({
                    'id': character.get('id'),
                    'realm': character.get('realm'),
                    'region': character.get('region')
                })
    
    all_matches_raw = []
    
    def fetch_player_data(player):
        try:
            p_id = player['id']
            rm_id = player['realm']
            rg_id = player['region']
            
            time.sleep(0.5) 
            data = fetch_match_history_raw(token, rg_id, rm_id, p_id)
            
            if data and data.get('matches'):
                return data
                
        except Exception:
            pass
        return None

    print(f"Extraindo matches para {len(all_players)} jogadores...")
    # Limita pra não explodir em desenvolvimento, testar de boa
    limited_players = all_players[:20]

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fetch_player_data, p): p for p in limited_players}
        for future in as_completed(futures):
            res = future.result()
            if res:
                if isinstance(res, list):
                    all_matches_raw.extend(res)
                else:
                    all_matches_raw.append(res)
            
    if all_matches_raw:
        out_file = f"{BRONZE_PATH}/matches_all_diamonds_history_{exec_date}.json"
        with open(out_file, 'w', encoding='utf-8') as f:
            json.dump(all_matches_raw, f, ensure_ascii=False)

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
