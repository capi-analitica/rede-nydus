import pytest
from unittest.mock import patch, MagicMock

from utils.get_token import get_battle_net_access_token


class TestGetBattleNetAccessToken:
    """Testes unitários para a função de autenticação OAuth2 da Battle.net."""

    def test_retorna_token_com_credenciais_validas(self):
        """Deve retornar o access_token como string quando a API responde com 200."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"access_token": "meu_token_valido", "token_type": "bearer"}
        mock_response.raise_for_status.return_value = None

        with patch("utils.get_token.requests.post", return_value=mock_response) as mock_post:
            token = get_battle_net_access_token("client_id_test", "client_secret_test")

        assert token == "meu_token_valido"
        mock_post.assert_called_once()

    def test_url_construida_corretamente_por_regiao(self):
        """A URL do endpoint deve mudar conforme a região informada."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"access_token": "token_eu"}
        mock_response.raise_for_status.return_value = None

        for region in ["us", "eu", "kr", "tw"]:
            with patch("utils.get_token.requests.post", return_value=mock_response) as mock_post:
                get_battle_net_access_token("id", "secret", region=region)
                call_args = mock_post.call_args
                url_chamada = call_args[0][0]
                assert url_chamada == f"https://{region}.battle.net/oauth/token"

    def test_payload_usa_client_credentials(self):
        """O corpo da requisição deve informar grant_type=client_credentials."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"access_token": "token"}
        mock_response.raise_for_status.return_value = None

        with patch("utils.get_token.requests.post", return_value=mock_response) as mock_post:
            get_battle_net_access_token("id", "secret")
            _, kwargs = mock_post.call_args
            assert kwargs["data"]["grant_type"] == "client_credentials"

    def test_retorna_none_em_http_401(self):
        """Deve retornar None (sem lançar exceção) quando a API responde com 401."""
        import requests as req
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.text = "Unauthorized"
        mock_response.raise_for_status.side_effect = req.exceptions.HTTPError(
            response=mock_response
        )

        with patch("utils.get_token.requests.post", return_value=mock_response):
            token = get_battle_net_access_token("id_invalido", "secret_invalido")

        assert token is None

    def test_retorna_none_em_connection_error(self):
        """Deve retornar None quando não há conexão com a internet."""
        import requests as req
        with patch("utils.get_token.requests.post", side_effect=req.exceptions.ConnectionError("Sem rede")):
            token = get_battle_net_access_token("id", "secret")

        assert token is None

    def test_retorna_none_em_timeout(self):
        """Deve retornar None quando a requisição expira."""
        import requests as req
        with patch("utils.get_token.requests.post", side_effect=req.exceptions.Timeout("Timeout")):
            token = get_battle_net_access_token("id", "secret")

        assert token is None

    def test_retorna_none_se_access_token_ausente_na_resposta(self):
        """Se a resposta JSON não contiver 'access_token', deve retornar None."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"error": "invalid_client"}
        mock_response.raise_for_status.return_value = None

        with patch("utils.get_token.requests.post", return_value=mock_response):
            token = get_battle_net_access_token("id", "secret")

        assert token is None
