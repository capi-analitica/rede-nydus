import sys
import os
import glob

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

sys.path.append('/opt/airflow/source')

BRONZE_PATH = '/opt/airflow/source/bronze'
SILVER_PATH = '/opt/airflow/source/silver'

# Janela de reprocessamento: últimos N arquivos por liga.
# Cobre falhas e variações de intervalo sem depender do relógio.
WINDOW_SIZE = 10

# Ligas disponíveis na API Blizzard SC2: Platinum=3, Diamond=4, Master=5, Grandmaster=6
LEAGUE_IDS = [3, 4, 5, 6]


def _process_and_load(bronze_pattern, silver_subdir, transform_fn, load_fn):
    """
    Utilitário reutilizado por todas as tasks:
    1. Pega os últimos WINDOW_SIZE arquivos que casam com bronze_pattern
    2. Para cada um, verifica silver.processed_files
    3. Se não processado: transforma, carrega no Postgres, registra como processado
    4. Se já processado: pula sem fazer nada
    """
    from utils.silver_loader import is_file_processed, mark_file_processed

    files = sorted(glob.glob(bronze_pattern))[-WINDOW_SIZE:]

    if not files:
        print(f"Nenhum arquivo encontrado para o padrão: {bronze_pattern}")
        return

    os.makedirs(f"{SILVER_PATH}/{silver_subdir}", exist_ok=True)

    for filepath in files:
        filename = os.path.basename(filepath)

        if is_file_processed(filename):
            print(f"Já processado, pulando: {filename}")
            continue

        print(f"Processando: {filename}")
        df = transform_fn(filepath)
        inserted = load_fn(df)
        mark_file_processed(filename)
        print(f"Concluído: {filename} -> {inserted} linhas inseridas")


def _process_league(**context):
    from utils.silver_transforms import transform_league_to_silver
    from utils.silver_loader import load_league_divisions
    for lid in LEAGUE_IDS:
        _process_and_load(
            bronze_pattern=f"{BRONZE_PATH}/league_raw_{lid}_*.json",
            silver_subdir="league_divisions",
            transform_fn=transform_league_to_silver,
            load_fn=load_league_divisions,
        )


def _process_modern_ladders(**context):
    from utils.silver_transforms import transform_modern_ladders_to_silver
    from utils.silver_loader import load_modern_ladder_teams
    for lid in LEAGUE_IDS:
        _process_and_load(
            bronze_pattern=f"{BRONZE_PATH}/modern_ladders_raw_{lid}_*.json",
            silver_subdir="modern_ladder_teams",
            transform_fn=transform_modern_ladders_to_silver,
            load_fn=load_modern_ladder_teams,
        )


def _process_legacy_ladders(**context):
    from utils.silver_transforms import transform_legacy_ladders_to_silver
    from utils.silver_loader import load_legacy_ladder_members
    for lid in LEAGUE_IDS:
        _process_and_load(
            bronze_pattern=f"{BRONZE_PATH}/legacy_ladders_raw_{lid}_*.json",
            silver_subdir="legacy_ladder_members",
            transform_fn=transform_legacy_ladders_to_silver,
            load_fn=load_legacy_ladder_members,
        )


default_args = {
    'owner': 'capitao_sc2',
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
}

with DAG(
    'silver_snapshots',
    default_args=default_args,
    schedule_interval='*/10 * * * *',
    catchup=False,
    max_active_runs=1,
    tags=['starcraft', 'esports', 'data_lake', 'silver'],
) as dag:

    process_league = PythonOperator(
        task_id='process_league',
        python_callable=_process_league,
    )

    process_modern = PythonOperator(
        task_id='process_modern_ladders',
        python_callable=_process_modern_ladders,
    )

    process_legacy = PythonOperator(
        task_id='process_legacy_ladders',
        python_callable=_process_legacy_ladders,
    )

    # As 3 pipelines são independentes — rodam em paralelo
    [process_league, process_modern, process_legacy]
