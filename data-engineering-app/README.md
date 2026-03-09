# Rede Nydus: StarCraft II Data Engineering Pipeline

## Resumo do Projeto
Este projeto é um pipeline de Engenharia de Dados focado no consumo das APIs oficiais da Blizzard (StarCraft II). O objetivo principal é extrair dados de performance, acompanhar variações estruturais das ligas e de MMR (Matchmaking Rating), além de armazenar históricos de partidas para análises posteriores e aplicação de regras de negócio.

O repositório está estruturado segundo práticas de engenharia de dados e orquestrado pelo **Apache Airflow**.

---

## Princípios de Arquitetura

O projeto adota a arquitetura em camadas **Medallion Architecture** (Bronze, Silver, Gold) fundamentada no formato **ELT** (Extract, Load, Transform).

*   **Camada Bronze:** Responsável pela extração do dado cru (*RAW*), em formato `JSON`. Seu design foca em garantir o salvamento exato do payload da API de origem. Nenhuma validação de negócio, tipagem explícita de colunas ou união de tabelas complexas ocorre neste momento. Isso minimiza a perda de dados em casos de flutuações, campos imprevistos ou falhas de formatação por parte da Blizzard.

*   **Camada Silver:** Responsável pela transformação, normalização e carga dos dados brutos em um banco relacional estruturado (PostgreSQL, schema `silver`). Aplica limpeza de tipos, achatamento de arrays aninhados (*flatten*), deduplicação via `ON CONFLICT DO NOTHING` e controle de idempotência por meio de uma tabela de log de arquivos processados (`silver.processed_files`). Não aplica regras de negócio — apenas garante a integridade e consultabilidade dos dados.

---

## Árvore do Projeto (Diretórios de Interesse)

```text
rede-nydus/
├── data-engineering-app/
│   ├── docker-compose.yml              # Container com configurações Airflow, BDs auxiliares
│   └── dags/
│       ├── dag_bronze_structure.py         # Coleta diária de estrutura das 4 ligas
│       ├── dag_bronze_mmr_tracker.py       # Snapshots de MMR a cada 10 minutos
│       ├── dag_bronze_match_history.py     # Coleta diária de histórico de partidas
│       ├── dag_silver_snapshots.py         # Transforma bronze → silver.league_divisions,
│       │                                   #   modern_ladder_teams, legacy_ladder_members
│       └── dag_silver_match_history.py     # Transforma bronze → silver.match_history
└── source/
    ├── bronze/                         # Data Lake: arquivos RAW JSON particionados por timestamp
    └── utils/                          # Controladores independentes para requisições e carga
        ├── get_token.py                # Gestão de OAUTH Access Token
        ├── get_league_data.py          # Requisições em nível global de Ligas/Tiers
        ├── get_ladder.py               # Processamento individual de Ladders
        ├── get_match_history.py        # Recuperação de informações históricas restritas
        ├── silver_transforms.py        # Funções de transformação Bronze → Silver (Pandas)
        └── silver_loader.py            # Funções de carga no PostgreSQL + controle de idempotência
```

---

## Documentação Técnica

### 1. DAGs da Camada Bronze

#### `dag_bronze_structure.py`
Pipeline de execução diária (`@daily`). Coleta a estrutura estática das ligas (divisões, ladder IDs) e os dados legacy (nomes, clãs) para todas as 4 ligas monitoradas: Platinum (3), Diamond (4), Master (5) e Grandmaster (6).

**Trilha de Execução (`Tasks`):**
1.  **`get_token`**: Autentica na Blizzard via OAuth2 e deposita o token no `XCom`.
2.  **`extract_leagues`**: Para cada uma das 4 ligas, acessa o endpoint de estrutura da API e salva `league_raw_{league_id}_{YYYY-MM-DD_HHMM}.json`. Retorna `{league_id: [ladder_ids]}` via `XCom`.
3.  **`extract_legacy_ladders`**: Para cada ladder mapeado, consulta a API legada (`/legacy/`) e salva `legacy_ladders_raw_{league_id}_{YYYY-MM-DD_HHMM}.json`. Contém variáveis qualitativas que persistem do motor antigo: Tag de Clã (`clanTag`), nome interno clássico e informações de membros por divisão.

---

#### `dag_bronze_mmr_tracker.py`
Pipeline de execução contínua (janelas de 10 minutos). Registra snapshots consecutivos de MMR para criar granularidade de flutuações intra-diárias de rating, sem consumir a cota da API com re-extração da estrutura.

**Trilha de Execução (`Tasks`):**
1.  **`get_token`**: Autentica na Blizzard via OAuth2.
2.  **`load_ladder_ids`**: Lê o arquivo `league_raw_{id}_*.json` mais recente do disco para cada liga. Evita chamadas desnecessárias à API — a estrutura de ligas já está disponível via `dag_bronze_structure`.
3.  **`extract_modern_ladders`**: Para cada liga, acessa o endpoint principal da API de Dados (`/data/`) e salva `modern_ladders_raw_{league_id}_{YYYY-MM-DD_HHMM}.json`. Contém métricas quantitativas: rating absoluto (MMR), estatísticas de partidas, BattleTag e ranking.

*Padrão de saída:* `modern_ladders_raw_{league_id}_{YYYY-MM-DD_HHMM}.json`

---

#### `dag_bronze_match_history.py`
Pipeline de execução diária. Coleta o histórico completo de partidas de todos os jogadores das 4 ligas (~28.700 jogadores únicos). Utiliza checkpoint a cada 500 jogadores para evitar perda de dados em caso de falha. Ao finalizar, dispara automaticamente o `dag_silver_match_history`.

**Trilha de Execução (`Tasks`):**
1.  **`get_token`**: Autentica na Blizzard via OAuth2.
2.  **`load_players`**: Lê os arquivos `legacy_ladders_raw_{id}_*.json` mais recentes, deduplica jogadores por `character_id` e salva `players_list_{exec_date}.json`.
3.  **`extract_matches`**: Para cada jogador, consulta o endpoint de histórico de partidas. Salva checkpoints a cada 500 resultados em `matches_batches_{exec_date}/batch_XXXX.json` e consolida em `matches_all_history_{exec_date}.json` ao final.
4.  **`trigger_silver_match_history`**: Dispara `dag_silver_match_history` via `TriggerDagRunOperator`.

---

### 2. DAGs da Camada Silver

A camada Silver é responsável por transformar os arquivos JSON brutos da camada Bronze em tabelas relacionais normalizadas no PostgreSQL (schema `silver`). O controle de idempotência é garantido pela tabela `silver.processed_files`, que registra cada arquivo já processado — impedindo reprocessamentos duplicados mesmo quando o DAG é executado frequentemente.

#### `dag_silver_snapshots.py`
Pipeline de execução contínua (10 minutos), sincronizada com o `dag_bronze_mmr_tracker`. Processa uma janela dos últimos 10 arquivos por liga (`WINDOW_SIZE = 10`), pulando os que já constam em `silver.processed_files`.

**Tasks (executadas em paralelo):**
- **`process_league`**: Transforma `league_raw_{id}_*.json` → `silver.league_divisions`
- **`process_modern_ladders`**: Transforma `modern_ladders_raw_{id}_*.json` → `silver.modern_ladder_teams`
- **`process_legacy_ladders`**: Transforma `legacy_ladders_raw_{id}_*.json` → `silver.legacy_ladder_members`

**Tabelas resultantes:**

| Tabela | Granularidade | Chave Primária |
|---|---|---|
| `silver.league_divisions` | 1 linha por divisão por snapshot | `(ladder_id, snapshot_ts)` |
| `silver.modern_ladder_teams` | 1 linha por jogador por ladder por snapshot | `(ladder_id, character_id, snapshot_ts)` |
| `silver.legacy_ladder_members` | 1 linha por membro por ladder por snapshot | `(ladder_id, character_id, snapshot_ts)` |

---

#### `dag_silver_match_history.py`
Pipeline de execução sob demanda (`schedule_interval=None`), disparada automaticamente ao fim de cada execução bem-sucedida do `dag_bronze_match_history`.

**Trilha de Execução (`Tasks`):**
1.  **`process_match_history`**: Lê o arquivo `matches_all_history_*.json` mais recente, achata a estrutura aninhada (1 linha por partida), converte timestamps Unix para `datetime` e retorna o DataFrame via `XCom`.
2.  **`load_to_postgres`**: Carrega o DataFrame transformado na tabela `silver.match_history` com `ON CONFLICT DO NOTHING`.

**Tabela resultante:**

| Tabela | Granularidade | Chave Primária |
|---|---|---|
| `silver.match_history` | 1 linha por partida por jogador | `(profile_id, realm_id, region_id, match_date)` |

---

### 3. Controle de Idempotência (`silver.processed_files`)

Todas as tasks da silver consultam a tabela `silver.processed_files` antes de processar qualquer arquivo:

```
silver.processed_files
  └── filename TEXT (PK)   -- nome do arquivo bronze (ex: modern_ladders_raw_4_2026-03-09_0510.json)
  └── processed_at TIMESTAMP
```

Se o arquivo já consta na tabela, a task pula o processamento silenciosamente. Ao concluir com sucesso, registra o nome do arquivo. Isso garante que re-execuções do DAG (por retry ou trigger manual) não gerem duplicatas nas tabelas silver.

---

## Guias e Restrições de Desenvolvimento

Para analistas que vierem a manter a estrutura:

1.  **Gestão de Rate Limits**: A cota estabelecida no Portal de Desenvolvedores da Blizzard é de até *36.000 requisições por hora*. O fracionamento de jobs (em ciclos de 10 minutos ou menos) foi arquitetado de modo que o pico não utilize mais que 10% dessa tolerância.

2.  **Isolamento da Ingestão / Arquitetura ELT**: As bibliotecas e funções em `source/utils/` sob a batuta da Ingestão Bronze não efetuam manipulação com Dataframes (Pandas) ou junções de estruturas. Cada extração guarda sua própria assinatura estrutural. O Join entre metadados legados (clãs) e metadados vigentes (ratings) é delegado exclusivamente à camada *Silver* em tempo de compilação analítica.

3.  **Segurança de Credenciais**: As chaves `CLIENT_ID` e `CLIENT_SECRET` não devem permanecer hardcoded (em texto plano) durantes stages de `Production`, devendo ser encapsuladas adequadamente por meio da tabela `Variables` acessada nativamente pelo Apache Airflow.

## RoadMap de Engenharia
*   ~~Criação da **Camada Silver**~~ ✅ Implementado — tabelas `league_divisions`, `modern_ladder_teams`, `legacy_ladder_members` e `match_history` no schema `silver` do PostgreSQL.
*   **Camada Gold (Data Warehouse):** Disponibilização de views ou tabelas agregadas para viabilizar ingestões BI ou aplicações diretas à lógica analítica (ex: evolução de MMR por jogador, taxa de vitória por raça, ranking histórico por liga).