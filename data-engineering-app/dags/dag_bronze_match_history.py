import sys
import os
import time
import json
import glob
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

# Tenta carregar o arquivo .env se estiver rodando localmente (fora do docker)
load_dotenv()

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

sys.path.append('/opt/airflow/source')

from utils.get_token import get_battle_net_access_token
from utils.get_match_history import fetch_match_history_raw

CLIENT_ID = os.getenv('BLIZZARD_CLIENT_ID', 'COLOQUE_SEU_CLIENT_ID_AQUI')
CLIENT_SECRET = os.getenv('BLIZZARD_CLIENT_SECRET', 'COLOQUE_SEU_SECRET_AQUI')
BRONZE_PATH = '/opt/airflow/source/bronze'

# Ligas cobertas: 3=Platinum, 4=Diamond, 5=Master, 6=Grandmaster
LEAGUE_IDS = [3, 4, 5, 6]

# Salva progresso no disco a cada N jogadores para não perder dados
# em execuções longas que possam ser interrompidas
CHECKPOINT_EVERY = 500


def _get_token(**context):
    token = get_battle_net_access_token(CLIENT_ID, CLIENT_SECRET)
    if not token:
        raise ValueError("Falha ao obter o token da Blizzard!")
    return token


def _load_players(**context):
    """
    Camada Bronze - Lê os arquivos legacy_ladders_raw_{league_id}_{date}.json do disco,
    gerados pelo dag_bronze_structure (@daily). Extrai e deduplica a lista de jogadores
    de todas as ligas (Platinum, Diamond, Master, Grandmaster).

    Depende de dag_bronze_structure ter rodado ao menos uma vez.
    Retorna lista de {id, realm, region} via XCom.
    """
    # Usa dict para deduplicar jogadores que aparecem em múltiplas ligas
    unique_players = {}

    for league_id in LEAGUE_IDS:
        files = sorted(glob.glob(f"{BRONZE_PATH}/legacy_ladders_raw_{league_id}_*.json"))
        if not files:
            print(f"Nenhum arquivo legacy_ladders_raw_{league_id} encontrado, pulando liga {league_id}.")
            continue

        latest = files[-1]
        print(f"Liga {league_id}: lendo {os.path.basename(latest)}")

        with open(latest, 'r', encoding='utf-8') as f:
            ladders_data = json.load(f)

        for ladder_entry in ladders_data:
            members = ladder_entry.get('data', {}).get('ladderMembers', [])
            for member in members:
                char = member.get('character', {})
                if char and char.get('id') and char.get('realm') and char.get('region'):
                    key = (char['id'], char['realm'], char['region'])
                    unique_players[key] = {
                        'id': char['id'],
                        'realm': char['realm'],
                        'region': char['region'],
                    }

    player_list = list(unique_players.values())
    print(f"Total de jogadores únicos para coleta de match history: {len(player_list)}")
    return player_list


def _extract_matches(**context):
    """
    Camada Bronze - Extrai match history de todos os jogadores via API da Blizzard.

    - Processa as 4 ligas sem limites artificiais.
    - Salva checkpoint a cada CHECKPOINT_EVERY jogadores: garante que nenhum
      dado coletado seja perdido em caso de falha ou timeout da execução.
    - Saída: matches_all_history_{exec_date}.json
    """
    token = context['ti'].xcom_pull(task_ids='get_token')
    players = context['ti'].xcom_pull(task_ids='load_players')
    exec_date = context['logical_date'].strftime('%Y-%m-%d_%H%M')
    out_file = f"{BRONZE_PATH}/matches_all_history_{exec_date}.json"

    os.makedirs(BRONZE_PATH, exist_ok=True)

    if not players:
        print("Nenhum jogador encontrado. Verifique se dag_bronze_structure já rodou.")
        return

    print(f"Iniciando extração de match history para {len(players)} jogadores...")

    all_matches_raw = []

    def fetch_player_matches(player):
        try:
            time.sleep(0.5)
            data = fetch_match_history_raw(
                token, player['region'], player['realm'], player['id']
            )
            if data and data.get('matches'):
                return data
        except Exception as e:
            print(f"Erro ao buscar matches do jogador {player['id']}: {e}")
        return None

    # Processa em lotes de CHECKPOINT_EVERY — salva progresso parcial após cada lote
    for batch_start in range(0, len(players), CHECKPOINT_EVERY):
        batch = players[batch_start: batch_start + CHECKPOINT_EVERY]
        batch_end = min(batch_start + CHECKPOINT_EVERY, len(players))
        print(f"[lote] Processando jogadores {batch_start + 1}–{batch_end} de {len(players)}...")

        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {executor.submit(fetch_player_matches, p): p for p in batch}
            for future in as_completed(futures):
                result = future.result()
                if result:
                    all_matches_raw.append(result)

        # Checkpoint: persiste o acumulado até aqui para não perder progresso
        with open(out_file, 'w', encoding='utf-8') as f:
            json.dump(all_matches_raw, f, ensure_ascii=False)
        print(f"Checkpoint salvo: {len(all_matches_raw)} registros em {out_file}")

    print(f"Extração concluída: {len(all_matches_raw)} jogadores com partidas coletadas.")


default_args = {
    'owner': 'capitao_sc2',
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
}

with DAG(
    'bronze_match_history_scraper',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['starcraft', 'esports', 'data_lake', 'match_history'],
) as dag:

    get_token = PythonOperator(
        task_id='get_token',
        python_callable=_get_token,
    )

    load_players = PythonOperator(
        task_id='load_players',
        python_callable=_load_players,
    )

    extract_matches = PythonOperator(
        task_id='extract_matches',
        python_callable=_extract_matches,
    )

    trigger_silver_match_history = TriggerDagRunOperator(
        task_id='trigger_silver_match_history',
        trigger_dag_id='silver_match_history',
        wait_for_completion=False,
    )

    get_token >> load_players >> extract_matches >> trigger_silver_match_history
