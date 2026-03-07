import requests
from requests.auth import HTTPBasicAuth

def get_battle_net_access_token(client_id: str, client_secret: str, region: str = 'us'):
    """
    Realiza uma requisição POST para a API OAuth2 da Battle.net para obter um
    access token usando o fluxo Client Credentials.

    :param client_id: Seu Client ID da Battle.net.
    :param client_secret: Seu Client Secret da Battle.net.
    :param region: A região do endpoint (ex: 'us', 'eu', 'kr', 'tw').
    :return: O access_token como string, ou None em caso de falha.
    """
    
    # 1. URL do endpoint de token
    # O formato da URL é https://<region>.battle.net/oauth/token
    token_url = f"https://{region}.battle.net/oauth/token"
    
    # 2. Dados do corpo da requisição (Form Data)
    # Para o fluxo Client Credentials, o tipo de concessão é 'client_credentials'.
    payload = {
        'grant_type': 'client_credentials'
    }
    
    # 3. Autenticação (Basic Auth)
    # O client_id e client_secret são passados usando Basic Authentication.
    auth = HTTPBasicAuth(client_id, client_secret)
    
    try:
        # 4. Realiza a requisição POST
        response = requests.post(token_url, data=payload, auth=auth)
        
        # 5. Verifica se a requisição foi bem-sucedida (código de status 200)
        response.raise_for_status()
        
        # 6. Extrai e retorna o access_token do corpo JSON da resposta
        data = response.json()
        return data.get('access_token')

    except requests.exceptions.HTTPError as errh:
        print(f"Erro HTTP: {errh}")
        print(f"Resposta do servidor: {response.text}")
    except requests.exceptions.ConnectionError as errc:
        print(f"Erro de Conexão: {errc}")
    except requests.exceptions.Timeout as errt:
        print(f"Tempo Esgotado (Timeout): {errt}")
    except requests.exceptions.RequestException as err:
        print(f"Ocorreu um erro: {err}")
    
    return None