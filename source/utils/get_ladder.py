import requests
import pandas as pd

def fetch_ladder_modern_raw(acesso, ladder_id, api_region='us'):
    """
    Busca dados brutos (Raw JSON) da Ladder moderna (Data API) que contém o MMR.
    Sem manipulações de Pandas. Padrão arquitetura Bronze.
    """
    token = acesso
    url = f"https://{api_region}.api.blizzard.com/data/sc2/ladder/{ladder_id}"
    
    headers = {
        'Authorization': f'Bearer {token}',
        'Accept': 'application/json'
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ Erro ao buscar ladder modern raw {ladder_id}: {e}")
        return None

def fetch_ladder_legacy_raw(acesso, ladder_id, region_id='1', api_region='us'):
    """
    Busca dados brutos (Raw JSON) da Ladder Legacy (para dados de Clã/Nomes Antigos).
    Sem manipulações de Pandas. Padrão arquitetura Bronze.
    """
    token = acesso
    url = f"https://{api_region}.api.blizzard.com/sc2/legacy/ladder/{region_id}/{ladder_id}"
    
    headers = {
        'Authorization': f'Bearer {token}',
        'Accept': 'application/json'
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ Erro ao buscar ladder legacy raw {ladder_id}: {e}")
        return None