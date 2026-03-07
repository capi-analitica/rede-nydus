import requests

def fetch_match_history_raw(token, region_id, realm_id, profile_id, api_region='us'):
    """
    Busca o histórico de partidas de um jogador específico na API Legacy do SC2
    e exporta os dados no formato raw JSON (Bronze layer).
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
        
        # Apenas pega o body puro
        data = response.json()
        
        # Opcional, para ajudar depois a mesclar: Injetar os IDs no nível do corpo
        data['request_profile_id'] = profile_id
        data['request_realm_id'] = realm_id
        data['request_region_id'] = region_id
        
        return data

    except requests.exceptions.RequestException as e:
        print(f"❌ Erro ao buscar o histórico de partidas do profile {profile_id}: {e}")
        return None
