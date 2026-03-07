# Rede Nydus: StarCraft II Data Engineering Pipeline

## Resumo do Projeto
Este projeto é um pipeline de Engenharia de Dados focado no consumo das APIs oficiais da Blizzard (StarCraft II). O objetivo principal é extrair dados de performance, acompanhar variações estruturais das ligas e de MMR (Matchmaking Rating), além de armazenar históricos de partidas para análises posteriores e aplicação de regras de negócio.

O repositório está estruturado segundo práticas de engenharia de dados e orquestrado pelo **Apache Airflow**.

---

## Princípios de Arquitetura

O projeto adota a arquitetura em camadas **Medallion Architecture** (Bronze, Silver, Gold) fundamentada no formato **ELT** (Extract, Load, Transform).

*   **Camada Bronze:** Responsável pela extração do dado cru (*RAW*), em formato `JSON`. Seu design foca em garantir o salvamento exato do payload da API de origem. Nenhuma validação de negócio, tipagem explícita de colunas ou união de tabelas complexas ocorre neste momento. Isso minimiza a perda de dados em casos de flutuações, campos imprevistos ou falhas de formatação por parte da Blizzard.

---

## Árvore do Projeto (Diretórios de Interesse)

```text
rede-nydus/
├── data-engineering-app/
│   ├── docker-compose.yml         # Container com configurações Airflow, BDs auxiliares
│   └── dags/
│       ├── dag_bronze_mmr_tracker.py    # Fluxo contínuo de extração de MMR/Ligas
│       └── dag_bronze_match_history.py  # Coleta estendida de detalhamento de partidas
└── source/
    ├── bronze/                    # Ponto de aterrissagem (Data Lake Storage) de arquivos RAW
    └── utils/                     # Controladores independentes para requisições
        ├── get_token.py           # Gestão de OAUTH Access Token
        ├── get_league_data.py     # Requisições em nível global de Ligas/Tiers
        ├── get_ladder.py          # Processamento individual de Ladders
        └── get_match_history.py   # Recuperação de informações históricas restritas
```

---

## Documentação Técnica

### 1. Funcionalidade da DAG Principal

#### `dag_bronze_mmr_tracker.py`
Pipeline configurada para execução constante (atualmente configurada em janelas de 10 minutos). Tem por objetivo registrar snapshots consecutivos para criar granularidade de eventuais flutuações intra-diárias de MMR.

**Trilha de Execução (`Tasks`):**
1.  **`get_token`**: Executa autenticação na Blizzard e deposita o token na área temporária (`XCom`) do Airflow.
2.  **`extract_league`**: Acessa endpoints de Liga (ex: Diamante) efetuando a captura da árvore primária para mapear todos os IDs de ladders (sub-divisões). O documento RAW da liga já é ingerido ao Storage e a lista de `ladder_ids` é passada ao estágio seguinte.
3.  **Ingestão Paralelizada (Branches)**:
    Devido à fragmentação da arquitetura oficial da API Blizzard, a task distribui-se em dois braços extraídos ao mesmo tempo:
    *   **`extract_modern_ladders`**: Acessa o endpoint principal da API de Dados (`/data/`). É onde as métricas quantitativas vigentes estão contidas (Rating absoluto, estatísticas limitadas de partidas, BattleTag).
    *   **`extract_legacy_ladders`**: Acessa a API legada (`/legacy/`). Endpoint mantido pelas limitações estruturais da nova versão, essencial por conter variáveis qualitativas de interface que ainda persistem do motor antigo do jogo, como Tag de Clã (`clanTag`) e Nome interno clássico.

*Padrão de Saída:* Persistência particionada por timestamp contendo blocos puramente transacionais (`all_ladders_raw_YYYY-MM-DD_HHMM.json`).

---

## Guias e Restrições de Desenvolvimento

Para analistas que vierem a manter a estrutura:

1.  **Gestão de Rate Limits**: A cota estabelecida no Portal de Desenvolvedores da Blizzard é de até *36.000 requisições por hora*. O fracionamento de jobs (em ciclos de 10 minutos ou menos) foi arquitetado de modo que o pico não utilize mais que 10% dessa tolerância.

2.  **Isolamento da Ingestão / Arquitetura ELT**: As bibliotecas e funções em `source/utils/` sob a batuta da Ingestão Bronze não efetuam manipulação com Dataframes (Pandas) ou junções de estruturas. Cada extração guarda sua própria assinatura estrutural. O Join entre metadados legados (clãs) e metadados vigentes (ratings) é delegado exclusivamente à camada *Silver* em tempo de compilação analítica.

3.  **Segurança de Credenciais**: As chaves `CLIENT_ID` e `CLIENT_SECRET` não devem permanecer hardcoded (em texto plano) durantes stages de `Production`, devendo ser encapsuladas adequadamente por meio da tabela `Variables` acessada nativamente pelo Apache Airflow.

## RoadMap de Engenharia
*   Criação da **Camada Silver**: Scripts intermédios orientados a Pandas para efetuar normalização, remoção de arrays sublinhados pelo raw system da API (Flat), e criação das tabelas cruzadas (Modern Keys left-join Legacy Keys).
*   Projeção Ouro (Data Warehouse): Disponibilização estruturada em PostgreSQL para viabilizar ingestões BI ou aplicações diretas à lógica corporativa.