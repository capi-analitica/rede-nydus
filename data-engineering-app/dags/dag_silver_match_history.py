import sys
import os
import glob

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

sys.path.append('/opt/airflow/source')

from utils.silver_transforms import transform_match_history_to_silver
from utils.silver_loader import load_match_history

BRONZE_PATH = '/opt/airflow/source/bronze'


def _process_match_history_to_silver(**context):
    """
    Encontra o arquivo matches_all_history_*.json mais recente no bronze,
    transforma em DataFrame flat (uma linha por partida) e retorna via XCom
    o caminho do arquivo processado.
    """
    files = sorted(glob.glob(f"{BRONZE_PATH}/matches_all_history_*.json"))
    if not files:
        raise FileNotFoundError(
            f"Nenhum arquivo matches_all_history_*.json encontrado em {BRONZE_PATH}. "
            "Execute a dag_bronze_match_history primeiro."
        )

    latest_file = files[-1]
    print(f"Processando: {latest_file}")

    df = transform_match_history_to_silver(latest_file)
    print(f"DataFrame gerado: {df.shape[0]} linhas, {df.shape[1]} colunas.")

    return latest_file


def _load_match_history_to_postgres(**context):
    """
    Carrega o arquivo de match history (transformado) na tabela silver.match_history.
    """
    bronze_file = context['ti'].xcom_pull(task_ids='process_match_history')

    df = transform_match_history_to_silver(bronze_file)
    inserted = load_match_history(df)
    print(f"Carga concluída: {inserted} novas partidas inseridas.")


default_args = {
    'owner': 'capitao_sc2',
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
}

# Sem schedule_interval — este DAG é acionado exclusivamente via
# TriggerDagRunOperator pela dag_bronze_match_history ao concluir com sucesso.
with DAG(
    'silver_match_history',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=['starcraft', 'esports', 'silver', 'match_history'],
) as dag:

    process_match_history = PythonOperator(
        task_id='process_match_history',
        python_callable=_process_match_history_to_silver,
    )

    load_to_postgres = PythonOperator(
        task_id='load_to_postgres',
        python_callable=_load_match_history_to_postgres,
    )

    process_match_history >> load_to_postgres
