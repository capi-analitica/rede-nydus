import requests
import pandas as pd


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

    all_data = []

    for i, tier in enumerate(data['tier']):
        # Normalizar as divisões do tier atual
        df_divisions = pd.json_normalize(tier['division'])
        
        # Adicionar informações do tier às divisões
        df_divisions['tier_index'] = i
        df_divisions['tier_min_rating'] = tier['min_rating']
        df_divisions['tier_max_rating'] = tier['max_rating']
        
        # Adicionar à lista
        all_data.append(df_divisions)

    # Concatenar todos os DataFrames em um só
    df_completo = pd.concat(all_data, ignore_index=True)

    # Adicionar informações globais da chave (temporada, liga, etc)
    df_completo['season_id'] = data['key']['season_id']
    df_completo['queue_id'] = data['key']['queue_id']
    df_completo['league_id'] = data['key']['league_id']

    # Armazenar o resultado na variável para uso posterior  
    return df_completo



