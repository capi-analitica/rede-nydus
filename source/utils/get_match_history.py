import requests
import pandas as pd

def fetch_match_history(token, region_id, realm_id, profile_id, api_region='us'):
    """
    Busca o histórico de partidas de um jogador específico na API Legacy do SC2
    e exporta os dados em um DataFrame.
    """
    
    # Endpoint: /sc2/legacy/profile/{regionId}/{realmId}/{profileId}/matches
    url = f"https://{api_region}.api.blizzard.com/sc2/legacy/profile/{region_id}/{realm_id}/{profile_id}/matches"
    
    headers = {
        'Authorization': f'Bearer {token}',
        'Accept': 'application/json'
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        # O histórico de partidas fica sob a chave 'matches'
        if 'matches' in data and data['matches']:
            df_matches = pd.json_normalize(data['matches'])
            
            # Adicionar identificadores do jogador para o contexto da tabela
            df_matches['profile_id'] = profile_id
            df_matches['realm_id'] = realm_id
            df_matches['region_id'] = region_id
            
            return df_matches
        else:
            print(f"⚠️ Nenhuma partida encontrada para o player {profile_id}.")
            return pd.DataFrame() # Retorna DataFrame vazio

    except requests.exceptions.RequestException as e:
        print(f"❌ Erro ao buscar o histórico de partidas do profile {profile_id}: {e}")
        return pd.DataFrame()
