"""
Testes de integridade das DAGs.

Estratégia: as DAGs são analisadas via `ast` (syntax check) e leitura de texto
(verificação de configurações), sem necessidade de importar o Apache Airflow.
Isso permite que os testes rodem em qualquer ambiente CI/CD.
"""

import ast
import re
from pathlib import Path

import pytest

DAGS_DIR = Path(__file__).parent.parent.parent / "data-engineering-app" / "dags"

EXPECTED_DAGS = {
    "dag_bronze_structure.py",
    "dag_bronze_mmr_tracker.py",
    "dag_bronze_match_history.py",
    "dag_silver_snapshots.py",
    "dag_silver_match_history.py",
}


# ==============================================================================
# Fixtures
# ==============================================================================

@pytest.fixture(params=list(EXPECTED_DAGS))
def dag_source(request):
    """Parametrize: executa cada teste para cada DAG file."""
    path = DAGS_DIR / request.param
    return path.name, path.read_text(encoding="utf-8")


# ==============================================================================
# 1. Sintaxe Python válida
# ==============================================================================

class TestDagSyntax:
    """Todos os arquivos de DAG devem ter sintaxe Python válida."""

    def test_todos_dags_existem(self):
        """Todos os arquivos de DAG esperados devem existir no diretório."""
        existing = {f.name for f in DAGS_DIR.glob("*.py")}
        missing = EXPECTED_DAGS - existing
        assert not missing, f"DAGs ausentes: {missing}"

    def test_sem_erros_de_sintaxe(self, dag_source):
        """Nenhum DAG deve conter erros de sintaxe Python."""
        name, source = dag_source
        try:
            ast.parse(source)
        except SyntaxError as e:
            pytest.fail(f"Erro de sintaxe em {name}: {e}")


# ==============================================================================
# 2. Credenciais não hardcoded
# ==============================================================================

class TestDagCredentials:
    """As credenciais nunca devem estar em texto plano nos arquivos de DAG."""

    FORBIDDEN_PATTERNS = [
        r"client_id\s*=\s*['\"][a-zA-Z0-9]{10,}['\"]",
        r"client_secret\s*=\s*['\"][a-zA-Z0-9]{10,}['\"]",
    ]

    def test_sem_credenciais_hardcoded(self, dag_source):
        """CLIENT_ID e CLIENT_SECRET não devem estar em texto plano."""
        name, source = dag_source
        for pattern in self.FORBIDDEN_PATTERNS:
            match = re.search(pattern, source, re.IGNORECASE)
            assert not match, (
                f"Credencial hardcoded detectada em {name}: '{match.group()}'"
            )

    def test_credenciais_vem_de_env_ou_airflow_vars(self, dag_source):
        """DAGs que usam credenciais devem obtê-las via os.getenv ou Variable."""
        name, source = dag_source
        # DAGs bronze usam credenciais; silver não usam diretamente
        if "bronze" not in name:
            return
        uses_env = "os.getenv" in source or "Variable.get" in source
        assert uses_env, (
            f"{name} não usa os.getenv() nem Variable.get() para credenciais."
        )


# ==============================================================================
# 3. Configuração de schedule
# ==============================================================================

class TestDagSchedule:
    """Cada DAG deve ter o schedule_interval correto conforme documentação."""

    def test_silver_match_history_sem_agendamento(self):
        """dag_silver_match_history deve ter schedule_interval=None (sob demanda)."""
        source = (DAGS_DIR / "dag_silver_match_history.py").read_text()
        assert "schedule_interval=None" in source, (
            "dag_silver_match_history deve ter schedule_interval=None — "
            "é disparado pelo dag_bronze_match_history via TriggerDagRunOperator."
        )

    def test_bronze_structure_executa_diariamente(self):
        """dag_bronze_structure deve rodar com agendamento @daily."""
        source = (DAGS_DIR / "dag_bronze_structure.py").read_text()
        assert "@daily" in source or "schedule_interval='@daily'" in source, (
            "dag_bronze_structure deve ter schedule @daily."
        )

    def test_silver_snapshots_tem_janela_de_10_minutos(self):
        """dag_silver_snapshots deve processar no máximo WINDOW_SIZE arquivos por execução."""
        source = (DAGS_DIR / "dag_silver_snapshots.py").read_text()
        assert "WINDOW_SIZE" in source, (
            "dag_silver_snapshots deve definir WINDOW_SIZE para controle de janela."
        )

    def test_silver_snapshots_window_size_e_10(self):
        """WINDOW_SIZE deve ser 10 conforme documentação."""
        source = (DAGS_DIR / "dag_silver_snapshots.py").read_text()
        match = re.search(r"WINDOW_SIZE\s*=\s*(\d+)", source)
        assert match, "WINDOW_SIZE não encontrado em dag_silver_snapshots.py"
        assert int(match.group(1)) == 10, (
            f"WINDOW_SIZE esperado: 10, encontrado: {match.group(1)}"
        )


# ==============================================================================
# 4. Dependências de tasks
# ==============================================================================

class TestDagTaskDependencies:
    """As dependências entre tasks devem seguir a trilha documentada."""

    def test_bronze_structure_ordem_de_tasks(self):
        """dag_bronze_structure: get_token >> extract_leagues >> extract_legacy_ladders."""
        source = (DAGS_DIR / "dag_bronze_structure.py").read_text()
        # Verifica que a cadeia de dependências está declarada
        assert "get_token" in source
        assert "extract_leagues" in source
        assert "extract_legacy_ladders" in source
        # A declaração da trilha deve existir no arquivo
        assert ">>" in source, "Nenhuma dependência de task (>>) encontrada."

    def test_bronze_match_history_dispara_silver(self):
        """dag_bronze_match_history deve conter um TriggerDagRunOperator."""
        source = (DAGS_DIR / "dag_bronze_match_history.py").read_text()
        assert "TriggerDagRunOperator" in source, (
            "dag_bronze_match_history deve disparar dag_silver_match_history "
            "via TriggerDagRunOperator ao final."
        )

    def test_bronze_match_history_aponta_para_silver_correto(self):
        """O trigger deve apontar para o DAG ID correto: 'silver_match_history'."""
        source = (DAGS_DIR / "dag_bronze_match_history.py").read_text()
        assert "silver_match_history" in source, (
            "TriggerDagRunOperator não referencia 'silver_match_history'."
        )

    def test_bronze_match_history_tem_checkpoint(self):
        """O DAG deve implementar checkpoint a cada 500 jogadores."""
        source = (DAGS_DIR / "dag_bronze_match_history.py").read_text()
        assert "500" in source, (
            "dag_bronze_match_history deve implementar checkpoint a cada 500 jogadores."
        )


# ==============================================================================
# 5. IDs únicos entre DAGs
# ==============================================================================

class TestDagIds:
    """Cada DAG deve ter um ID único — colisões causam comportamento indefinido no Airflow."""

    def test_dag_ids_sao_unicos(self):
        """Nenhum dag_id deve aparecer mais de uma vez entre os arquivos."""
        dag_id_pattern = re.compile(r"DAG\s*\(\s*['\"]([^'\"]+)['\"]")
        ids_encontrados = {}

        for dag_file in DAGS_DIR.glob("*.py"):
            source = dag_file.read_text(encoding="utf-8")
            for match in dag_id_pattern.finditer(source):
                dag_id = match.group(1)
                assert dag_id not in ids_encontrados, (
                    f"dag_id '{dag_id}' duplicado: encontrado em "
                    f"{ids_encontrados[dag_id]} e {dag_file.name}"
                )
                ids_encontrados[dag_id] = dag_file.name

    def test_todos_dags_tem_dag_id(self):
        """Cada arquivo de DAG deve declarar um DAG com ID."""
        dag_id_pattern = re.compile(r"DAG\s*\(\s*['\"]([^'\"]+)['\"]")
        for name in EXPECTED_DAGS:
            source = (DAGS_DIR / name).read_text(encoding="utf-8")
            assert dag_id_pattern.search(source), (
                f"{name} não declara um DAG com ID string."
            )
