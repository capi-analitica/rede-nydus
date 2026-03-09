import json
import shutil
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from utils.silver_transforms import (
    _parse_snapshot_ts,
    _parse_league_id,
    transform_league_to_silver,
    transform_modern_ladders_to_silver,
    transform_legacy_ladders_to_silver,
    transform_match_history_to_silver,
)

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


# ==============================================================================
# Helpers para criar arquivos temporários com o nome correto
# ==============================================================================

def _write_fixture(tmp_dir: str, filename: str, fixture_name: str) -> str:
    """Copia um fixture para um diretório temporário com o nome esperado."""
    src = FIXTURES_DIR / fixture_name
    dst = Path(tmp_dir) / filename
    shutil.copy(src, dst)
    return str(dst)


# ==============================================================================
# _parse_snapshot_ts
# ==============================================================================

class TestParseSnapshotTs:
    """Testes para a extração de timestamp a partir do nome do arquivo."""

    def test_formato_novo_multi_liga(self):
        """Extrai corretamente data e hora no formato moderno com league_id."""
        ts = _parse_snapshot_ts("modern_ladders_raw_4_2026-03-07_0440.json")
        assert ts == "2026-03-07 04:40:00"

    def test_formato_legado_sem_league_id(self):
        """Extrai corretamente data e hora no formato legado (sem league_id)."""
        ts = _parse_snapshot_ts("modern_ladders_raw_2026-03-07_0440.json")
        assert ts == "2026-03-07 04:40:00"

    def test_formato_com_caminho_absoluto(self):
        """Deve funcionar corretamente com caminhos completos de arquivo."""
        ts = _parse_snapshot_ts("/opt/airflow/source/bronze/league_raw_4_2026-03-09_1200.json")
        assert ts == "2026-03-09 12:00:00"


# ==============================================================================
# _parse_league_id
# ==============================================================================

class TestParseLeagueId:
    """Testes para a extração de league_id a partir do nome do arquivo."""

    def test_extrai_league_id_do_nome(self):
        """Deve extrair o league_id numérico do nome do arquivo moderno."""
        assert _parse_league_id("modern_ladders_raw_4_2026-03-07_0440.json") == 4
        assert _parse_league_id("legacy_ladders_raw_6_2026-03-07_0440.json") == 6
        assert _parse_league_id("league_raw_3_2026-03-09_1010.json") == 3

    def test_fallback_para_diamond_em_arquivo_legado(self):
        """Para arquivos sem league_id no nome, o fallback deve ser 4 (Diamond)."""
        assert _parse_league_id("modern_ladders_raw_2026-03-07_0440.json") == 4


# ==============================================================================
# transform_league_to_silver
# ==============================================================================

class TestTransformLeagueToSilver:
    """Testes para a transformação bronze -> silver de estrutura de ligas."""

    def test_colunas_obrigatorias_presentes(self, tmp_path):
        """O DataFrame resultante deve conter todas as colunas do schema silver."""
        path = _write_fixture(str(tmp_path), "league_raw_4_2026-03-07_0440.json", "league_raw.json")
        df = transform_league_to_silver(path)
        expected_cols = {
            "season_id", "queue_id", "league_id", "team_type",
            "tier_id", "ladder_id", "snapshot_ts",
        }
        assert expected_cols.issubset(set(df.columns))

    def test_uma_linha_por_divisao(self, tmp_path):
        """Cada divisão do JSON deve gerar exatamente uma linha no DataFrame."""
        path = _write_fixture(str(tmp_path), "league_raw_4_2026-03-07_0440.json", "league_raw.json")
        df = transform_league_to_silver(path)
        # fixture: 2 divisões no tier 0 + 1 no tier 1 = 3 linhas
        assert len(df) == 3

    def test_snapshot_ts_e_datetime(self, tmp_path):
        """A coluna snapshot_ts deve ser do tipo datetime64."""
        path = _write_fixture(str(tmp_path), "league_raw_4_2026-03-07_0440.json", "league_raw.json")
        df = transform_league_to_silver(path)
        assert pd.api.types.is_datetime64_any_dtype(df["snapshot_ts"])

    def test_ladder_ids_corretos(self, tmp_path):
        """Os ladder_ids do DataFrame devem corresponder aos do JSON."""
        path = _write_fixture(str(tmp_path), "league_raw_4_2026-03-07_0440.json", "league_raw.json")
        df = transform_league_to_silver(path)
        assert set(df["ladder_id"]) == {101, 102, 201}

    def test_json_vazio_retorna_dataframe_vazio(self, tmp_path):
        """Um JSON sem tiers deve retornar um DataFrame vazio sem lançar exceção."""
        empty_json = tmp_path / "league_raw_4_2026-03-07_0440.json"
        empty_json.write_text('{"key": {}, "tier": []}')
        df = transform_league_to_silver(str(empty_json))
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0

    def test_snapshot_ts_extraido_do_nome_do_arquivo(self, tmp_path):
        """O valor de snapshot_ts deve vir do nome do arquivo, não do JSON."""
        path = _write_fixture(str(tmp_path), "league_raw_4_2026-03-07_0440.json", "league_raw.json")
        df = transform_league_to_silver(path)
        assert df["snapshot_ts"].iloc[0] == pd.Timestamp("2026-03-07 04:40:00")


# ==============================================================================
# transform_modern_ladders_to_silver
# ==============================================================================

class TestTransformModernLaddersToSilver:
    """Testes para a transformação bronze -> silver de snapshots MMR."""

    def test_colunas_obrigatorias_presentes(self, tmp_path):
        """O DataFrame deve conter os campos fundamentais de identidade e métrica."""
        path = _write_fixture(str(tmp_path), "modern_ladders_raw_4_2026-03-07_0440.json", "modern_ladders_raw.json")
        df = transform_modern_ladders_to_silver(path)
        expected_cols = {"league_id", "ladder_id", "character_id", "rating", "snapshot_ts"}
        assert expected_cols.issubset(set(df.columns))

    def test_uma_linha_por_membro(self, tmp_path):
        """Cada membro de cada time deve gerar uma linha no DataFrame."""
        path = _write_fixture(str(tmp_path), "modern_ladders_raw_4_2026-03-07_0440.json", "modern_ladders_raw.json")
        df = transform_modern_ladders_to_silver(path)
        # fixture: ladder 101 tem 1 membro, ladder 102 tem 2 membros = 3 linhas
        assert len(df) == 3

    def test_snapshot_ts_e_datetime(self, tmp_path):
        """A coluna snapshot_ts deve ser do tipo datetime64."""
        path = _write_fixture(str(tmp_path), "modern_ladders_raw_4_2026-03-07_0440.json", "modern_ladders_raw.json")
        df = transform_modern_ladders_to_silver(path)
        assert pd.api.types.is_datetime64_any_dtype(df["snapshot_ts"])

    def test_league_id_extraido_do_nome_do_arquivo(self, tmp_path):
        """O league_id deve vir do nome do arquivo."""
        path = _write_fixture(str(tmp_path), "modern_ladders_raw_4_2026-03-07_0440.json", "modern_ladders_raw.json")
        df = transform_modern_ladders_to_silver(path)
        assert (df["league_id"] == 4).all()

    def test_team_id_armazenado_como_string(self, tmp_path):
        """team_id deve ser string para evitar overflow de uint64."""
        path = _write_fixture(str(tmp_path), "modern_ladders_raw_4_2026-03-07_0440.json", "modern_ladders_raw.json")
        df = transform_modern_ladders_to_silver(path)
        assert df["team_id"].dtype == object  # object = string no pandas

    def test_primary_race_preenchido(self, tmp_path):
        """primary_race deve ser extraído da lista played_race_count."""
        path = _write_fixture(str(tmp_path), "modern_ladders_raw_4_2026-03-07_0440.json", "modern_ladders_raw.json")
        df = transform_modern_ladders_to_silver(path)
        player_one = df[df["character_id"] == 1234567]
        assert player_one.iloc[0]["primary_race"] == "Zerg"

    def test_json_vazio_retorna_dataframe_vazio(self, tmp_path):
        """Uma lista vazia deve retornar um DataFrame vazio sem lançar exceção."""
        empty_json = tmp_path / "modern_ladders_raw_4_2026-03-07_0440.json"
        empty_json.write_text("[]")
        df = transform_modern_ladders_to_silver(str(empty_json))
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0


# ==============================================================================
# transform_legacy_ladders_to_silver
# ==============================================================================

class TestTransformLegacyLaddersToSilver:
    """Testes para a transformação bronze -> silver de dados legados (clãs)."""

    def test_colunas_obrigatorias_presentes(self, tmp_path):
        """O DataFrame deve conter character_id, clan_tag e snapshot_ts."""
        path = _write_fixture(str(tmp_path), "legacy_ladders_raw_4_2026-03-07_0440.json", "legacy_ladders_raw.json")
        df = transform_legacy_ladders_to_silver(path)
        expected_cols = {"character_id", "clan_tag", "ladder_id", "snapshot_ts"}
        assert expected_cols.issubset(set(df.columns))

    def test_uma_linha_por_membro(self, tmp_path):
        """Cada membro por ladder deve gerar uma linha no DataFrame."""
        path = _write_fixture(str(tmp_path), "legacy_ladders_raw_4_2026-03-07_0440.json", "legacy_ladders_raw.json")
        df = transform_legacy_ladders_to_silver(path)
        # fixture: ladder 101 tem 1 membro, ladder 102 tem 1 membro = 2 linhas
        assert len(df) == 2

    def test_snapshot_ts_e_datetime(self, tmp_path):
        """snapshot_ts deve ser datetime64."""
        path = _write_fixture(str(tmp_path), "legacy_ladders_raw_4_2026-03-07_0440.json", "legacy_ladders_raw.json")
        df = transform_legacy_ladders_to_silver(path)
        assert pd.api.types.is_datetime64_any_dtype(df["snapshot_ts"])

    def test_clan_tag_nulo_aceito(self, tmp_path):
        """Membros sem clan_tag não devem causar erro — o campo aceita null."""
        path = _write_fixture(str(tmp_path), "legacy_ladders_raw_4_2026-03-07_0440.json", "legacy_ladders_raw.json")
        df = transform_legacy_ladders_to_silver(path)
        player_two = df[df["character_id"] == 7654321]
        assert pd.isna(player_two.iloc[0]["clan_tag"])

    def test_json_vazio_retorna_dataframe_vazio(self, tmp_path):
        """Uma lista vazia deve retornar um DataFrame vazio sem lançar exceção."""
        empty_json = tmp_path / "legacy_ladders_raw_4_2026-03-07_0440.json"
        empty_json.write_text("[]")
        df = transform_legacy_ladders_to_silver(str(empty_json))
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0


# ==============================================================================
# transform_match_history_to_silver
# ==============================================================================

class TestTransformMatchHistoryToSilver:
    """Testes para a transformação bronze -> silver do histórico de partidas."""

    def test_colunas_obrigatorias_presentes(self, tmp_path):
        """O DataFrame deve conter os campos da chave primária e dados da partida."""
        path = _write_fixture(str(tmp_path), "matches_all_history_2026-03-07_0440.json", "matches_all_history.json")
        df = transform_match_history_to_silver(path)
        expected_cols = {"profile_id", "realm_id", "region_id", "match_date", "map", "decision"}
        assert expected_cols.issubset(set(df.columns))

    def test_uma_linha_por_partida_por_jogador(self, tmp_path):
        """Cada partida de cada jogador deve virar uma linha separada."""
        path = _write_fixture(str(tmp_path), "matches_all_history_2026-03-07_0440.json", "matches_all_history.json")
        df = transform_match_history_to_silver(path)
        # fixture: jogador 1 tem 2 partidas, jogador 2 tem 1, jogador 3 tem 0 = 3 linhas
        assert len(df) == 3

    def test_match_date_e_datetime(self, tmp_path):
        """O timestamp Unix deve ser convertido para datetime64."""
        path = _write_fixture(str(tmp_path), "matches_all_history_2026-03-07_0440.json", "matches_all_history.json")
        df = transform_match_history_to_silver(path)
        assert pd.api.types.is_datetime64_any_dtype(df["match_date"])

    def test_match_date_nao_e_inteiro(self, tmp_path):
        """match_date não pode ser um inteiro — deve ser datetime."""
        path = _write_fixture(str(tmp_path), "matches_all_history_2026-03-07_0440.json", "matches_all_history.json")
        df = transform_match_history_to_silver(path)
        assert not pd.api.types.is_integer_dtype(df["match_date"])

    def test_jogador_sem_partidas_nao_gera_linhas(self, tmp_path):
        """Jogador com lista de partidas vazia não deve gerar nenhuma linha."""
        path = _write_fixture(str(tmp_path), "matches_all_history_2026-03-07_0440.json", "matches_all_history.json")
        df = transform_match_history_to_silver(path)
        # profile_id 9999999 tem matches=[] na fixture
        rows_sem_match = df[df["profile_id"] == 9999999]
        assert len(rows_sem_match) == 0

    def test_json_vazio_retorna_dataframe_vazio(self, tmp_path):
        """Um arquivo com lista vazia deve retornar DataFrame vazio sem erro."""
        empty_json = tmp_path / "matches_all_history_2026-03-07_0440.json"
        empty_json.write_text("[]")
        df = transform_match_history_to_silver(str(empty_json))
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0
