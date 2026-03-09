import pandas as pd
import pytest
from unittest.mock import MagicMock, patch, call


# ==============================================================================
# Estratégia de teste: todo acesso ao banco (psycopg2) é mockado.
# Não há dependência de uma instância PostgreSQL real.
# ==============================================================================


def _make_mock_conn(fetchone_return=None):
    """
    Cria um mock completo do contexto de conexão psycopg2.
    Retorna (conn_mock, cursor_mock) para inspeção nas asserções.
    """
    cursor_mock = MagicMock()
    cursor_mock.fetchone.return_value = fetchone_return
    cursor_mock.rowcount = -1  # psycopg2 retorna -1 para execute_values por padrão

    cursor_ctx = MagicMock()
    cursor_ctx.__enter__ = MagicMock(return_value=cursor_mock)
    cursor_ctx.__exit__ = MagicMock(return_value=False)

    conn_mock = MagicMock()
    conn_mock.cursor.return_value = cursor_ctx
    conn_mock.__enter__ = MagicMock(return_value=conn_mock)
    conn_mock.__exit__ = MagicMock(return_value=False)

    return conn_mock, cursor_mock


# ==============================================================================
# is_file_processed
# ==============================================================================

class TestIsFileProcessed:
    """Testes para o controle de idempotência via silver.processed_files."""

    def test_retorna_true_se_arquivo_ja_processado(self):
        """Deve retornar True quando o arquivo consta na tabela processed_files."""
        from utils.silver_loader import is_file_processed

        conn_mock, cursor_mock = _make_mock_conn(fetchone_return=(1,))

        with patch("utils.silver_loader._get_connection", return_value=conn_mock):
            result = is_file_processed("modern_ladders_raw_4_2026-03-09_0510.json")

        assert result is True

    def test_retorna_false_se_arquivo_nao_processado(self):
        """Deve retornar False quando o arquivo NÃO consta na tabela."""
        from utils.silver_loader import is_file_processed

        conn_mock, cursor_mock = _make_mock_conn(fetchone_return=None)

        with patch("utils.silver_loader._get_connection", return_value=conn_mock):
            result = is_file_processed("arquivo_novo.json")

        assert result is False

    def test_consulta_com_nome_correto_do_arquivo(self):
        """A query deve ser executada com o nome exato do arquivo como parâmetro."""
        from utils.silver_loader import is_file_processed

        conn_mock, cursor_mock = _make_mock_conn(fetchone_return=None)

        target_filename = "legacy_ladders_raw_4_2026-03-09_0510.json"
        with patch("utils.silver_loader._get_connection", return_value=conn_mock):
            is_file_processed(target_filename)

        # Verifica que o SELECT foi chamado com o nome correto
        execute_calls = cursor_mock.execute.call_args_list
        select_call = next(c for c in execute_calls if "processed_files" in str(c) and "SELECT" in str(c))
        assert target_filename in str(select_call)


# ==============================================================================
# mark_file_processed
# ==============================================================================

class TestMarkFileProcessed:
    """Testes para o registro de arquivos processados."""

    def test_executa_insert_com_nome_do_arquivo(self):
        """Deve executar um INSERT com o nome do arquivo informado."""
        from utils.silver_loader import mark_file_processed

        conn_mock, cursor_mock = _make_mock_conn()

        filename = "modern_ladders_raw_4_2026-03-09_0510.json"
        with patch("utils.silver_loader._get_connection", return_value=conn_mock):
            mark_file_processed(filename)

        insert_calls = [c for c in cursor_mock.execute.call_args_list if "INSERT" in str(c)]
        assert len(insert_calls) == 1
        assert filename in str(insert_calls[0])

    def test_usa_on_conflict_do_nothing(self):
        """O INSERT deve conter ON CONFLICT DO NOTHING para evitar erros em re-runs."""
        from utils.silver_loader import mark_file_processed

        conn_mock, cursor_mock = _make_mock_conn()

        with patch("utils.silver_loader._get_connection", return_value=conn_mock):
            mark_file_processed("qualquer_arquivo.json")

        insert_sql = str(cursor_mock.execute.call_args_list)
        assert "ON CONFLICT DO NOTHING" in insert_sql.upper() or "on conflict do nothing" in insert_sql.lower()


# ==============================================================================
# load_league_divisions
# ==============================================================================

class TestLoadLeagueDivisions:
    """Testes para a carga de dados de divisões de liga."""

    def _make_df(self):
        return pd.DataFrame([{
            "season_id": 66, "queue_id": 201, "league_id": 4, "team_type": 0,
            "tier_id": 0, "tier_min_rating": 4000, "tier_max_rating": 5000,
            "division_id": 0, "ladder_id": 101, "member_count": 50,
            "snapshot_ts": pd.Timestamp("2026-03-07 04:40:00"),
        }])

    def test_chama_execute_values(self):
        """A carga deve usar execute_values do psycopg2 para inserção em batch."""
        from utils.silver_loader import load_league_divisions

        conn_mock, cursor_mock = _make_mock_conn()

        with patch("utils.silver_loader._get_connection", return_value=conn_mock), \
             patch("utils.silver_loader.psycopg2.extras.execute_values") as mock_ev:
            load_league_divisions(self._make_df())

        mock_ev.assert_called_once()

    def test_sql_contem_on_conflict_do_nothing(self):
        """O SQL de INSERT deve conter ON CONFLICT DO NOTHING."""
        from utils.silver_loader import load_league_divisions

        conn_mock, cursor_mock = _make_mock_conn()
        captured_sql = []

        def capture_execute_values(cur, sql, rows, **kwargs):
            captured_sql.append(sql)
            cur.rowcount = len(rows)

        with patch("utils.silver_loader._get_connection", return_value=conn_mock), \
             patch("utils.silver_loader.psycopg2.extras.execute_values", side_effect=capture_execute_values):
            load_league_divisions(self._make_df())

        assert any("ON CONFLICT" in sql.upper() for sql in captured_sql)
        assert any("DO NOTHING" in sql.upper() for sql in captured_sql)

    def test_dataframe_vazio_nao_levanta_excecao(self):
        """Passar um DataFrame vazio não deve causar erro."""
        from utils.silver_loader import load_league_divisions

        conn_mock, _ = _make_mock_conn()
        empty_df = pd.DataFrame(columns=self._make_df().columns)

        with patch("utils.silver_loader._get_connection", return_value=conn_mock), \
             patch("utils.silver_loader.psycopg2.extras.execute_values"):
            try:
                load_league_divisions(empty_df)
            except Exception as e:
                pytest.fail(f"Exceção inesperada com DataFrame vazio: {e}")


# ==============================================================================
# load_modern_ladder_teams
# ==============================================================================

class TestLoadModernLadderTeams:
    """Testes para a carga de times modernos (MMR)."""

    def _make_df(self):
        return pd.DataFrame([{
            "league_id": 4, "ladder_id": 101, "team_id": "9900000000001",
            "rating": 4200, "wins": 10, "losses": 5, "ties": 0, "points": 100,
            "longest_win_streak": 5, "current_win_streak": 2, "current_rank": 1,
            "highest_rank": 1, "previous_rank": 2, "join_time_stamp": 1700000000,
            "last_played_time_stamp": 1741000000, "character_id": 1234567,
            "character_realm": 1, "character_name": "Player", "battle_tag": "P#1",
            "primary_race": "Zerg", "primary_race_count": 15,
            "snapshot_ts": pd.Timestamp("2026-03-07 04:40:00"),
        }])

    def test_filtra_linhas_sem_character_id(self):
        """Linhas com character_id nulo devem ser removidas antes do INSERT."""
        from utils.silver_loader import load_modern_ladder_teams

        df = self._make_df()
        df_com_nulo = pd.concat([
            df,
            df.assign(character_id=None),
        ], ignore_index=True)

        conn_mock, _ = _make_mock_conn()
        captured_rows = []

        def capture(cur, sql, rows, **kwargs):
            captured_rows.extend(rows)
            cur.rowcount = len(rows)

        with patch("utils.silver_loader._get_connection", return_value=conn_mock), \
             patch("utils.silver_loader.psycopg2.extras.execute_values", side_effect=capture):
            load_modern_ladder_teams(df_com_nulo)

        # Apenas a linha com character_id válido deve ser inserida
        assert len(captured_rows) == 1

    def test_sql_contem_on_conflict_do_nothing(self):
        """O SQL deve conter ON CONFLICT DO NOTHING."""
        from utils.silver_loader import load_modern_ladder_teams

        conn_mock, _ = _make_mock_conn()
        captured_sql = []

        def capture(cur, sql, rows, **kwargs):
            captured_sql.append(sql)
            cur.rowcount = len(rows)

        with patch("utils.silver_loader._get_connection", return_value=conn_mock), \
             patch("utils.silver_loader.psycopg2.extras.execute_values", side_effect=capture):
            load_modern_ladder_teams(self._make_df())

        assert any("ON CONFLICT" in sql.upper() for sql in captured_sql)


# ==============================================================================
# load_match_history
# ==============================================================================

class TestLoadMatchHistory:
    """Testes para a carga do histórico de partidas."""

    def _make_df(self):
        return pd.DataFrame([{
            "profile_id": 1234567, "realm_id": 1, "region_id": 1,
            "match_date": pd.Timestamp("2026-03-07 04:26:40"),
            "map": "Equilibrium", "type": "SOLO", "decision": "WIN",
            "speed": "FASTER", "snapshot_date": "2026-03-07",
        }])

    def test_filtra_linhas_sem_profile_id_ou_match_date(self):
        """Linhas sem profile_id ou match_date devem ser removidas antes do INSERT."""
        from utils.silver_loader import load_match_history

        df = self._make_df()
        df_com_nulos = pd.concat([
            df,
            df.assign(profile_id=None),
            df.assign(match_date=None),
        ], ignore_index=True)

        conn_mock, _ = _make_mock_conn()
        captured_rows = []

        def capture(cur, sql, rows, **kwargs):
            captured_rows.extend(rows)
            cur.rowcount = len(rows)

        with patch("utils.silver_loader._get_connection", return_value=conn_mock), \
             patch("utils.silver_loader.psycopg2.extras.execute_values", side_effect=capture):
            load_match_history(df_com_nulos)

        # Apenas a linha com profile_id E match_date preenchidos deve ser inserida
        assert len(captured_rows) == 1

    def test_sql_contem_on_conflict_do_nothing(self):
        """O SQL deve conter ON CONFLICT DO NOTHING."""
        from utils.silver_loader import load_match_history

        conn_mock, _ = _make_mock_conn()
        captured_sql = []

        def capture(cur, sql, rows, **kwargs):
            captured_sql.append(sql)
            cur.rowcount = len(rows)

        with patch("utils.silver_loader._get_connection", return_value=conn_mock), \
             patch("utils.silver_loader.psycopg2.extras.execute_values", side_effect=capture):
            load_match_history(self._make_df())

        assert any("ON CONFLICT" in sql.upper() for sql in captured_sql)
