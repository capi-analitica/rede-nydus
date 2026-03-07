import requests

def get_league_data_raw(season_id, queue_id, team_type, league_id, token, region='us'):
    """
    Faz a requisição ao endpoint /data/sc2/league/ e apenas devolve o JSON original sem manipulação.
    Este é o padrão strict para a Camada Bronze.
    """
    url = f"https://{region}.api.blizzard.com/data/sc2/league/{season_id}/{queue_id}/{team_type}/{league_id}"
    
    headers = {
        'Authorization': f'Bearer {token}',
        'Accept': 'application/json'
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    
    return response.json()



