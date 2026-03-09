import pytest
from unittest.mock import patch, MagicMock

from utils.get_league_data import get_league_data_raw
from utils.get_ladder import fetch_ladder_modern_raw, fetch_ladder_legacy_raw


class TestGetLeagueDataRaw:
    """Testes unitários para a função de consulta de estrutura de ligas."""

    def test_retorna_json_bruto_sem_modificacoes(self):
        """A função deve retornar exatamente o JSON recebido da API, sem transformações."""
        payload_api = {"key": {"league_id": 4}, "tier": [{"id": 0, "division": []}]}
        mock_response = MagicMock()
        mock_response.json.return_value = payload_api
        mock_response.raise_for_status.return_value = None

        with patch("utils.get_league_data.requests.get", return_value=mock_response):
            result = get_league_data_raw(
                season_id=66, queue_id=201, team_type=0, league_id=4, token="tok"
            )

        assert result == payload_api

    def test_url_montada_com_todos_os_parametros(self):
        """A URL da requisição deve conter season_id, queue_id, team_type e league_id."""
        mock_response = MagicMock()
        mock_response.json.return_value = {}
        mock_response.raise_for_status.return_value = None

        with patch("utils.get_league_data.requests.get", return_value=mock_response) as mock_get:
            get_league_data_raw(season_id=66, queue_id=201, team_type=0, league_id=4, token="tok")
            url = mock_get.call_args[0][0]

        assert "66" in url
        assert "201" in url
        assert "/0/" in url
        assert "/4" in url

    def test_url_muda_conforme_regiao(self):
        """O parâmetro region deve alterar o domínio da URL."""
        mock_response = MagicMock()
        mock_response.json.return_value = {}
        mock_response.raise_for_status.return_value = None

        with patch("utils.get_league_data.requests.get", return_value=mock_response) as mock_get:
            get_league_data_raw(66, 201, 0, 4, token="tok", region="eu")
            url = mock_get.call_args[0][0]

        assert "eu.api.blizzard.com" in url

    def test_header_authorization_presente(self):
        """O header Authorization com Bearer token deve estar na requisição."""
        mock_response = MagicMock()
        mock_response.json.return_value = {}
        mock_response.raise_for_status.return_value = None

        with patch("utils.get_league_data.requests.get", return_value=mock_response) as mock_get:
            get_league_data_raw(66, 201, 0, 4, token="meu_token_secreto")
            headers = mock_get.call_args[1]["headers"]

        assert headers["Authorization"] == "Bearer meu_token_secreto"

    def test_propaga_http_error(self):
        """Deve propagar HTTPError para erros de API (ex: token expirado)."""
        import requests as req
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = req.exceptions.HTTPError("403")

        with patch("utils.get_league_data.requests.get", return_value=mock_response):
            with pytest.raises(req.exceptions.HTTPError):
                get_league_data_raw(66, 201, 0, 4, token="tok_invalido")


class TestFetchLadderModernRaw:
    """Testes unitários para a busca de dados MMR da ladder moderna."""

    def test_retorna_json_bruto(self):
        """Deve retornar o JSON da API sem nenhuma manipulação."""
        payload = {"team": [{"id": 123, "rating": 4200}]}
        mock_response = MagicMock()
        mock_response.json.return_value = payload
        mock_response.raise_for_status.return_value = None

        with patch("utils.get_ladder.requests.get", return_value=mock_response):
            result = fetch_ladder_modern_raw(acesso="token", ladder_id=101)

        assert result == payload

    def test_url_contem_ladder_id(self):
        """A URL deve conter o ladder_id informado."""
        mock_response = MagicMock()
        mock_response.json.return_value = {}
        mock_response.raise_for_status.return_value = None

        with patch("utils.get_ladder.requests.get", return_value=mock_response) as mock_get:
            fetch_ladder_modern_raw(acesso="token", ladder_id=9876)
            url = mock_get.call_args[0][0]

        assert "9876" in url
        assert "/data/sc2/ladder/" in url

    def test_header_authorization_presente(self):
        """O header Authorization com Bearer token deve estar presente."""
        mock_response = MagicMock()
        mock_response.json.return_value = {}
        mock_response.raise_for_status.return_value = None

        with patch("utils.get_ladder.requests.get", return_value=mock_response) as mock_get:
            fetch_ladder_modern_raw(acesso="token_xyz", ladder_id=101)
            headers = mock_get.call_args[1]["headers"]

        assert headers["Authorization"] == "Bearer token_xyz"

    def test_retorna_none_em_falha_de_rede(self):
        """Deve retornar None sem lançar exceção em caso de falha de rede."""
        import requests as req
        with patch("utils.get_ladder.requests.get", side_effect=req.exceptions.ConnectionError()):
            result = fetch_ladder_modern_raw(acesso="token", ladder_id=101)

        assert result is None

    def test_sem_manipulacao_pandas(self):
        """A função não deve retornar um DataFrame — apenas JSON bruto."""
        import pandas as pd
        mock_response = MagicMock()
        mock_response.json.return_value = {"team": []}
        mock_response.raise_for_status.return_value = None

        with patch("utils.get_ladder.requests.get", return_value=mock_response):
            result = fetch_ladder_modern_raw(acesso="token", ladder_id=101)

        assert not isinstance(result, pd.DataFrame)


class TestFetchLadderLegacyRaw:
    """Testes unitários para a busca de dados Legacy (clãs, nomes) da ladder."""

    def test_retorna_json_bruto(self):
        """Deve retornar o JSON da API legacy sem modificações."""
        payload = {"ladderMembers": [{"character": {"id": 123}}]}
        mock_response = MagicMock()
        mock_response.json.return_value = payload
        mock_response.raise_for_status.return_value = None

        with patch("utils.get_ladder.requests.get", return_value=mock_response):
            result = fetch_ladder_legacy_raw(acesso="token", ladder_id=101)

        assert result == payload

    def test_url_contem_legacy_e_ladder_id(self):
        """A URL deve apontar para o endpoint /legacy/ e conter o ladder_id."""
        mock_response = MagicMock()
        mock_response.json.return_value = {}
        mock_response.raise_for_status.return_value = None

        with patch("utils.get_ladder.requests.get", return_value=mock_response) as mock_get:
            fetch_ladder_legacy_raw(acesso="token", ladder_id=5555)
            url = mock_get.call_args[0][0]

        assert "legacy" in url
        assert "5555" in url

    def test_retorna_none_em_falha_de_rede(self):
        """Deve retornar None sem lançar exceção em caso de falha de rede."""
        import requests as req
        with patch("utils.get_ladder.requests.get", side_effect=req.exceptions.RequestException()):
            result = fetch_ladder_legacy_raw(acesso="token", ladder_id=101)

        assert result is None
