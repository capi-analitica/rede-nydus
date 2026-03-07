import requests
import pandas as pd



def get_season_data(acesso, region_id=1, api_region='us'):
    """
    Busca a temporada atual do SC2 para uma região específica.
    region_id: 1=US, 2=EU, 3=KR
    """
    token = acesso

    # Endpoint: /data/sc2/season/{regionId}
    url = f"https://{api_region}.api.blizzard.com/sc2/ladder/season/{region_id}"
    
    headers = {'Authorization': f'Bearer {token}'}
    params = {
        'namespace': f'dynamic-{api_region}',
        'locale': 'en_US'
    }

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        saida = pd.DataFrame([data])

        return saida

    except requests.exceptions.RequestException as e:
        print(f"❌ Erro: {e}")