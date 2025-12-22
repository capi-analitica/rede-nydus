import requests


def league_data(season_id, queue_id, team_type, league_id, token ,region='us'):
    """
    Faz a requisição ao endpoint /data/sc2/league/ e exporta para JSON.
    """
    # 2. Configurar a URL e Headers
    # Endpoint: /data/sc2/league/{seasonId}/{queueId}/{teamType}/{leagueId}
    url = f"https://{region}.api.blizzard.com/data/sc2/league/{season_id}/{queue_id}/{team_type}/{league_id}"
    
    headers = {
        'Authorization': f'Bearer {token}',
        'Accept': 'application/json'
    }
    # 3. Realizar a requisição
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    
    return data
