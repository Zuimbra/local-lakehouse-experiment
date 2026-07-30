# 🏗️ Lakehouse Experiment
> Projeto para teste dos conceitos de Data Lake e Lakehouse


## 🎯 Objetivo

Testar, estudar e exemplificar a estrutura do **Data Lakehouse** (Delta Table), passando antes pelo **Data Lake** (Parquet Puro).

## 🏛️ Arquitetura

```
data/
├── raw/                    ← CSV original (incluído no repositório)
├── lake/                   ← Data Lake (Parquet puro)
│   ├── 01_bronze/          ← Dados brutos em Parquet
│   ├── 02_silver/          ← Dados limpos (Star Schema)
│   └── 03_gold/            ← Agregações para BI
└── lakehouse/              ← Data Lakehouse (Delta Table)
    ├── 01_bronze/          ← Dados brutos em Delta Table
    ├── 02_silver/          ← Dados limpos (Star Schema)
    └── 03_gold/            ← Agregações para BI
```

### Comparativo

| Feature | Data Lake (Parquet) | Lakehouse (Delta Table) |
|---|---|---|
| Formato | `.parquet` | `.parquet` + `_delta_log/` |
| Transações ACID | ❌ | ✅ |
| Time Travel | ❌ | ✅ |
| Schema Enforcement | ❌ | ✅ |
| Upserts/Merges | Manual | Nativo |

## 🛠️ Tech Stack

- **Python 3.14**
- **DuckDB** — Motor de consulta SQL local
- **Delta Lake** (`deltalake`) — Formato de tabela transacional
- **Pandas / PyArrow** — Manipulação de dados
- **uv** — Gerenciador de pacotes

## 🚀 Como Rodar

### 1. Instalar dependências

```bash
uv sync
```

### 2. Executar os pipelines

```bash
# Pipeline Data Lake (Parquet)
uv run src/lake_pipeline.py

# Pipeline Data Lakehouse (Delta Table)
uv run src/lakehouse_pipeline.py
```

Ou execute cada camada individualmente:

```bash
# Data Lake
uv run src/lake_01_bronze.py
uv run src/lake_02_silver.py
uv run src/lake_03_gold.py

# Data Lakehouse
uv run src/lakehouse_01_bronze.py
uv run src/lakehouse_02_silver.py
uv run src/lakehouse_03_gold.py
```