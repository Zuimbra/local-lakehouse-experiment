# Local Lakehouse

Este projeto apresenta uma implementação local de uma arquitetura **Data Lakehouse voltada ao processamento de dados de rastreadores veiculares**. A proposta é simular, em um ambiente simples e executável localmente, um fluxo de engenharia de dados capaz de receber arquivos brutos, validar sua estrutura, preservar sua origem, tratar os registros, consolidar informações relevantes e disponibilizar os resultados para aplicações externas.

Mais do que apenas converter arquivos CSV em tabelas, o projeto busca resolver problemas que aparecem naturalmente quando o volume e a frequência dos dados começam a crescer. É necessário saber, por exemplo, se um arquivo já foi processado, evitar que uma mesma linha seja inserida duas vezes, identificar de onde determinado registro surgiu, separar arquivos defeituosos de registros semanticamente inválidos e atualizar somente os dados realmente afetados por uma nova ingestão.

Para isso, a solução utiliza a arquitetura **Medallion**, dividindo o processamento entre as camadas Bronze, Silver e Gold. Cada uma assume uma responsabilidade específica: a Bronze preserva e registra os dados recebidos; a Silver interpreta e valida esses registros; e a Gold organiza as informações de forma adequada para consultas, dashboards e outras aplicações.

O resultado é um pipeline incremental que aproxima um ambiente local de conceitos encontrados em plataformas modernas de dados, incluindo **Delta Lake, controle de ingestão, linhagem, idempotência, processamento incremental, late-arriving data e exposição dos dados por API REST**.

---

## Visão geral

O funcionamento pode ser resumido da seguinte forma:

```text
Arquivos CSV de rastreadores
             │
             ▼
    Validação estrutural
       ┌─────┴─────┐
       │           │
     válido     inválido
       │           │
       │           ▼
       │       Quarantine
       │
       ▼
 Controle de ingestão
       │
       ▼
     Bronze
 dados preservados
       │
       ▼
     Silver
 dados tratados
       │
       ▼
      Gold
 dados consolidados
       │
       ▼
   FastAPI / REST
       │
       ▼
Dashboard • MCP • Aplicações
```

Em termos simples:

> **os arquivos entram como dados brutos e saem como informações estruturadas e prontas para consumo.**

---

## O que o projeto trata

Um fluxo de dados contínuo envolve mais do que simplesmente ler arquivos.

A arquitetura foi construída para responder questões como:

```text
┌──────────────────────────────────────────────────────┐
│ O mesmo arquivo já foi processado?                   │
│                                                      │
│ Esta linha já existe no Lakehouse?                   │
│                                                      │
│ De qual arquivo determinado registro veio?           │
│                                                      │
│ O arquivo possui uma estrutura válida?               │
│                                                      │
│ Uma linha inválida deve invalidar o arquivo inteiro? │
│                                                      │
│ Um arquivo antigo chegou atrasado?                   │
│                                                      │
│ É necessário reconstruir todo o Lakehouse?           │
└──────────────────────────────────────────────────────┘
```

Esses problemas são tratados em diferentes etapas do pipeline.

---

# Arquitetura

```text
                           RAW
                            │
                 data/raw/inbox/
                            │
                            ▼
                  Validação estrutural
                     │           │
                  válido      inválido
                     │           │
                     │           ▼
                     │      quarantine/
                     │      + error report
                     │
                     ▼
                00_control
              ingestion_files
                     │
                     ▼
                   BRONZE
                 tracker_logs
                     │
                     ▼
                   SILVER
          ┌──────────┼───────────┐
          │          │           │
          ▼          ▼           ▼
     telemetry    identity    rejected
      events       events       logs
          │          │           │
          └──────────┼───────────┘
                     │
                     ▼
                    GOLD
       ┌─────────────┼───────────────┐
       │             │               │
       ▼             ▼               ▼
    devices       positions       analytics
       │
       └─────────────┬───────────────┘
                     │
                     ▼
                  FastAPI
```

---

# Tecnologias

| Tecnologia                | Função                                   |
| ------------------------- | ---------------------------------------- |
| **Python**                | implementação do pipeline                |
| **Delta Lake / delta-rs** | armazenamento transacional               |
| **DuckDB**                | consultas e transformações               |
| **PyArrow**               | schemas e processamento columnar         |
| **Pandas**                | operações auxiliares                     |
| **FastAPI**               | API REST                                 |
| **Uvicorn**               | servidor ASGI                            |
| **uv**                    | gerenciamento do ambiente e dependências |

---

# Estrutura do projeto

```text
.
├── data/
│   ├── raw/
│   │   ├── inbox/
│   │   ├── archive/
│   │   └── quarantine/
│   │
│   └── lakehouse/
│       ├── 00_control/
│       │   └── ingestion_files/
│       │
│       ├── 01_bronze/
│       │   └── tracker_logs/
│       │
│       ├── 02_silver/
│       │   ├── telemetry_events/
│       │   ├── device_identity_events/
│       │   └── rejected_logs/
│       │
│       └── 03_gold/
│           ├── dim_device/
│           ├── device_last_position/
│           ├── device_route_points/
│           ├── device_daily_summary/
│           └── data_quality_summary/
│
├── src/
│   ├── lakehouse_01_bronze.py
│   ├── lakehouse_02_silver.py
│   ├── lakehouse_03_gold.py
│   ├── lakehouse_pipeline.py
│   └── api/
│
├── tests/
├── pyproject.toml
└── uv.lock
```

---

# Fluxo dos arquivos

Os arquivos que precisam ser processados entram em:

```text
data/raw/inbox/
```

Exemplo:

```text
inbox/
├── logs_2026-08-10.csv
├── logs_2026-08-11.csv
└── logs_2026-08-12.csv
```

Depois disso existem dois fluxos possíveis.

### Arquivo aceito

```text
inbox/
   │
   ▼
validação
   │
   ▼
Bronze
   │
   ▼
archive/
```

### Arquivo rejeitado

```text
inbox/
   │
   ▼
validação
   │
   ✕
   ▼
quarantine/
     +
arquivo.csv.error.txt
```

---

# Validação estrutural

Antes de entrar no Lakehouse, cada arquivo passa por uma validação inicial.

São verificados principalmente:

* leitura do CSV;
* arquivo não vazio;
* presença das colunas obrigatórias;
* ausência de colunas internas reservadas.

Colunas adicionais são permitidas.

```text
Colunas obrigatórias presentes
            +
       colunas extras
            │
            ▼
       Arquivo válido
```

Enquanto:

```text
Coluna obrigatória ausente
            │
            ▼
       Quarantine
```

A validação desta etapa é **estrutural**.

Ela responde:

> O arquivo possui condições mínimas para ser ingerido?

As regras de negócio são tratadas posteriormente pela Silver.

---

# Controle de ingestão

A tabela:

```text
data/lakehouse/00_control/ingestion_files
```

mantém o histórico das execuções de ingestão.

Ela registra informações como:

| Informação            | Finalidade                    |
| --------------------- | ----------------------------- |
| `source_file`         | arquivo processado            |
| `source_file_hash`    | identificação pelo conteúdo   |
| `batch_id`            | execução responsável          |
| `status`              | estado da ingestão            |
| `row_count`           | quantidade de linhas          |
| `inserted_row_count`  | registros novos               |
| `duplicate_row_count` | registros já existentes       |
| `error_message`       | erro ocorrido                 |
| timestamps            | início e fim do processamento |

Estados principais:

```text
PROCESSING
    │
    ├──► SUCCESS
    │
    ├──► FAILED
    │
    └──► SKIPPED
```

Essa tabela permite responder rapidamente:

```text
Esse arquivo já entrou?
Qual batch o processou?
Quantas linhas foram inseridas?
Existiam duplicados?
A execução falhou?
```

---

# Identificação de arquivos

Cada arquivo recebe um hash SHA-256 baseado em seu conteúdo.

```text
arquivo.csv
    │
    ▼
conteúdo do arquivo
    │
    ▼
SHA-256
    │
    ▼
source_file_hash
```

O nome do arquivo não define sua identidade.

Portanto:

```text
logs_original.csv
logs_copia.csv
```

podem ser reconhecidos como o mesmo conteúdo caso possuam o mesmo hash.

---

# Bronze

A Bronze representa a camada de preservação dos dados.

Tabela:

```text
data/lakehouse/01_bronze/tracker_logs
```

Ela mantém as informações recebidas e adiciona metadados de rastreabilidade.

## Metadados adicionados

| Campo               | Função                       |
| ------------------- | ---------------------------- |
| `source_file`       | arquivo de origem            |
| `source_file_hash`  | hash do arquivo              |
| `source_row_number` | linha no arquivo original    |
| `row_id`            | identificador determinístico |
| `batch_id`          | batch de ingestão            |
| `ingested_at`       | instante da ingestão         |
| `ingestion_date`    | partição física              |

---

## Linhagem

Um registro pode ser rastreado até sua origem:

```text
Registro
   │
   ├── source_file
   │      └── logs_2026-08-10.csv
   │
   ├── source_row_number
   │      └── linha 153
   │
   ├── batch_id
   │      └── execução responsável
   │
   └── ingested_at
          └── momento da ingestão
```

Isso fornece **data lineage** entre os dados armazenados e sua origem.

---

## `row_id`

O `row_id` identifica uma linha de forma determinística.

```text
source_file_hash
        +
source_row_number
        │
        ▼
      SHA-256
        │
        ▼
      row_id
```

Assim:

```text
mesmo arquivo
+
mesma linha
      │
      ▼
mesmo row_id
```

mesmo que o pipeline seja executado novamente.

---

# Idempotência

A Bronze utiliza `MERGE` do Delta Lake.

```text
                 novo registro
                       │
                       ▼
                comparar row_id
                   ┌───┴───┐
                   │       │
                existe   não existe
                   │       │
                   ▼       ▼
                ignorar   INSERT
```

O comportamento é um:

```text
MERGE insert-only
```

Isso evita que reprocessamentos criem registros duplicados.

---

# Silver

A Silver transforma dados brutos em informações de domínio.

Diretório:

```text
data/lakehouse/02_silver/
```

A camada produz:

```text
┌──────────────────────────┐
│ telemetry_events         │
├──────────────────────────┤
│ device_identity_events   │
├──────────────────────────┤
│ rejected_logs            │
└──────────────────────────┘
```

---

# Normalização

Antes da classificação dos registros:

```text
strings
   │
   ▼
trim
   │
   ▼
vazios → NULL
   │
   ▼
timestamps
   │
   ▼
TRY_CAST
   │
   ▼
valores numéricos
   │
   ▼
tipagem
```

O timestamp efetivo do evento segue a lógica:

```text
device_timestamp
        │
        ├── válido ───────► usar
        │
        └── inválido
               │
               ▼
       server_timestamp
```

Conceitualmente:

```text
event_timestamp =
COALESCE(device_timestamp, server_timestamp)
```

---

# Classificação das mensagens

```text
                   Registro
                      │
                      ▼
               tipo da mensagem
              ┌───────┼──────────┐
              │       │          │
              ▼       ▼          ▼
             T1     T2/T3/...  inválida
              │       │          │
              ▼       ▼          ▼
          identity  telemetry   rejected
```

---

# `telemetry_events`

Contém eventos operacionais dos rastreadores.

Exemplos:

```text
device_serial
event_timestamp
latitude
longitude
speed
direction
battery
odometer
horimeter
hdop
network
temperature
rpm
```

Partição:

```text
event_date
```

---

# Qualidade da posição

As coordenadas são verificadas antes de serem consideradas válidas.

```text
Latitude
-90 ─────────────── 90

Longitude
-180 ───────────────────────────── 180
```

Classificações possíveis:

```text
VALID
INVALID_COORDINATES
MISSING_COORDINATES
LOW_GPS_PRECISION
```

Um HDOP elevado pode indicar baixa precisão do GPS.

---

# `device_identity_events`

Mensagens de identidade são armazenadas separadamente.

Principalmente:

```text
T1
 │
 ▼
device_identity_events
```

Entre os identificadores extraídos estão:

```text
IMEI
IMSI
ICCID
```

Esses campos também passam por validações de formato.

---

# `rejected_logs`

Uma linha pode possuir estrutura válida para entrar na Bronze, mas não atender às regras da Silver.

Nesse caso:

```text
Bronze
   │
   ▼
validação semântica
   │
   ✕
   ▼
rejected_logs
```

Exemplos:

```text
MISSING_MESSAGE_TYPE
INVALID_MESSAGE_TYPE
MISSING_OR_INVALID_TIMESTAMP
MISSING_DEVICE_SERIAL
```

O registro é preservado para:

* auditoria;
* diagnóstico;
* qualidade de dados;
* investigação de falhas na origem.

---

# Quarantine x Rejected Logs

```text
┌───────────────────────────┬───────────────────────────┐
│        QUARANTINE         │       REJECTED_LOGS       │
├───────────────────────────┼───────────────────────────┤
│ Problema no arquivo       │ Problema em uma linha     │
│                           │                           │
│ O arquivo não entra       │ O arquivo entra           │
│ na Bronze                 │ normalmente na Bronze     │
│                           │                           │
│ Validação estrutural      │ Validação de negócio      │
└───────────────────────────┴───────────────────────────┘
```

Exemplo:

```text
CSV sem coluna obrigatória
        ↓
quarantine
```

Enquanto:

```text
CSV válido
   ↓
Bronze
   ↓
linha sem timestamp
   ↓
rejected_logs
```

---

# Processamento incremental da Silver

A Bronze informa quais batches receberam novos dados.

```text
Bronze
  │
  ▼
batch_ids
  │
  ▼
Silver
  │
  ▼
identificar datas afetadas
```

Exemplo:

```text
batch X
   │
   ├── evento 2026-08-10
   └── evento 2026-08-11
           │
           ▼
affected_event_dates
    ├── 2026-08-10
    └── 2026-08-11
```

Somente essas partições precisam ser reconstruídas.

---

# Late-arriving data

Um arquivo pode chegar depois da data de seus eventos.

```text
Hoje
2026-08-12

Novo arquivo
eventos de 2026-08-10
```

O sistema identifica:

```text
affected_event_date
        │
        ▼
   2026-08-10
```

e reconstrói somente esse período.

```text
Bronze completa da data
          +
 novos registros atrasados
          │
          ▼
nova partição Silver
```

As demais datas permanecem intactas.

---

# Modos da Silver

```text
┌─────────────┬────────────────────────────────────┐
│ FULL        │ Reconstrói toda a camada           │
├─────────────┼────────────────────────────────────┤
│ INCREMENTAL │ Atualiza apenas datas afetadas     │
├─────────────┼────────────────────────────────────┤
│ NOOP        │ Nenhuma alteração necessária       │
└─────────────┴────────────────────────────────────┘
```

---

# Gold

A Gold transforma eventos tratados em estruturas orientadas ao consumo.

Diretório:

```text
data/lakehouse/03_gold/
```

Tabelas:

```text
Gold
 │
 ├── dim_device
 ├── device_last_position
 ├── device_route_points
 ├── device_daily_summary
 └── data_quality_summary
```

---

# `dim_device`

Visão consolidada dos dispositivos.

```text
1 dispositivo
      │
      ▼
    1 linha
```

Pode reunir:

* primeira atividade;
* última atividade;
* IMEI;
* IMSI;
* ICCID;
* quantidade de eventos;
* presença de telemetria;
* presença de identidade.

---

# `device_last_position`

Mantém a última posição válida conhecida.

```text
Dispositivo
     │
     ▼
eventos históricos
     │
     ▼
posição válida mais recente
     │
     ▼
device_last_position
```

Permite responder rapidamente:

> Onde o dispositivo foi visto pela última vez?

---

# `device_route_points`

Representa os pontos utilizados para reconstruir trajetos.

```text
1 evento válido
        │
        ▼
1 ponto de rota
```

Principais informações:

```text
device_serial
event_timestamp
latitude
longitude
speed
direction
odometer
hdop
```

Partição:

```text
event_date
```

---

# `device_daily_summary`

Resumo por dispositivo e por dia.

```text
device_serial
      +
event_date
      │
      ▼
1 linha
```

Exemplo:

```text
DEVICE-001 | 2026-08-10
DEVICE-001 | 2026-08-11
DEVICE-002 | 2026-08-10
```

Métricas possíveis:

* quantidade de eventos;
* primeiro e último evento;
* posições válidas;
* posições inválidas;
* velocidade média;
* velocidade máxima;
* HDOP;
* hodômetro;
* eventos em movimento.

---

# `data_quality_summary`

Consolida indicadores de qualidade.

```text
1 dia
  │
  ▼
1 resumo de qualidade
```

Principais métricas:

```text
telemetry_event_count
identity_event_count
accepted_event_count
rejected_event_count
total_event_count
rejection_percentage
```

---

# Gold incremental

A Silver informa quais períodos foram alterados.

```text
Silver
  │
  ▼
datas afetadas
  │
  ├──► descobrir dispositivos afetados
  │
  └──► atualizar partições afetadas
```

Existem dois tipos principais de atualização.

### Por dispositivo

```text
dim_device
device_last_position
```

Fluxo:

```text
dispositivo afetado
        ↓
recalcular estado
        ↓
Delta MERGE
```

### Por data

```text
device_route_points
device_daily_summary
data_quality_summary
```

Fluxo:

```text
data afetada
      ↓
recalcular
      ↓
replace seletivo
```

---

# Orquestração

O pipeline principal está em:

```text
src/lakehouse_pipeline.py
```

Fluxo:

```text
┌─────────────────────┐
│  load_bronze_data   │
└──────────┬──────────┘
           │
           ▼
   BronzeLoadResult
           │
        batch_ids
           │
           ▼
┌─────────────────────┐
│  load_silver_data   │
└──────────┬──────────┘
           │
           ▼
   SilverLoadResult
           │
     affected dates
           │
           ▼
┌─────────────────────┐
│   load_gold_data    │
└─────────────────────┘
```

Cada camada informa explicitamente à próxima o que foi alterado.

---

# Execução sem novos dados

Se nenhum novo registro for inserido:

```text
mesmos arquivos
      │
      ▼
Bronze identifica
      │
      ▼
0 novos registros
      │
      ▼
has_new_data = False
      │
      ▼
NOOP
```

Silver e Gold não precisam ser executadas novamente.

---

# Executando o projeto

## Pré-requisitos

```text
Python
uv
```

Instale as dependências:

```bash
uv sync
```

---

## Adicionando dados

Coloque os CSVs em:

```text
data/raw/inbox/
```

---

## Executando o pipeline

```bash
uv run src/lakehouse_pipeline.py
```

Fluxo:

```text
Bronze
  ↓
Silver
  ↓
Gold
```

Essa é a execução recomendada.

---

## Executando individualmente

Bronze:

```bash
uv run src/lakehouse_01_bronze.py
```

Silver:

```bash
uv run src/lakehouse_02_silver.py
```

Gold:

```bash
uv run src/lakehouse_03_gold.py
```

A execução isolada das camadas pode utilizar reconstrução completa por não possuir o contexto incremental produzido pela etapa anterior.

---

# API REST

Inicie a aplicação:

```bash
uv run uvicorn src.api.main:app --reload
```

Servidor:

```text
http://127.0.0.1:8000
```

Swagger:

```text
http://127.0.0.1:8000/docs
```

---

# Endpoints

## Health

```http
GET /health
```

Verifica se a aplicação está executando.

---

## Readiness

```http
GET /ready
```

Verifica se as principais tabelas Gold estão disponíveis.

```text
/health
   │
   └── aplicação está ativa

/ready
   │
   └── aplicação + dados disponíveis
```

---

## Qualidade dos dados

```http
GET /api/v1/data-quality
```

Filtros:

```text
date_from
date_to
```

Data específica:

```http
GET /api/v1/data-quality/{metric_date}
```

---

## Resumo diário

```http
GET /api/v1/daily-summary
```

Filtros:

```text
date_from
date_to
device_serial
```

Também:

```http
GET /api/v1/daily-summary/{event_date}
```

---

## Rotas

```http
GET /api/v1/routes/{event_date}
```

Filtro opcional:

```text
device_serial
```

O retorno utiliza GeoJSON:

```text
FeatureCollection
```

Coordenadas:

```text
[longitude, latitude]
```

---

# Testes

Execute:

```bash
uv run python -m unittest discover -s tests -p "test_*.py" -v
```

Principais cenários cobertos:

```text
✓ múltiplos arquivos
✓ validação estrutural
✓ arquivos vazios
✓ quarantine
✓ hashes determinísticos
✓ row_id determinístico
✓ idempotência
✓ Delta MERGE
✓ tabela de controle
✓ processamento incremental
✓ late-arriving data
✓ replace seletivo
✓ FULL
✓ INCREMENTAL
✓ NOOP
```

---

# Exemplo ponta a ponta

```text
1. Arquivo chega
        │
        ▼
data/raw/inbox/logs.csv
        │
        ▼
2. Validação estrutural
        │
        ▼
3. Hash + batch
        │
        ▼
4. Bronze
        │
        ├── adiciona linhagem
        └── evita duplicados
        │
        ▼
5. Silver
        │
        ├── telemetria
        ├── identidade
        └── rejeições
        │
        ▼
6. Gold
        │
        ├── dispositivos
        ├── posições
        ├── rotas
        ├── resumo diário
        └── qualidade
        │
        ▼
7. FastAPI
        │
        ▼
Dashboard / MCP / consumidor externo
```

---

# Principais características

| Área                 | Implementação              |
| -------------------- | -------------------------- |
| Ingestão             | múltiplos CSVs             |
| Controle             | tabela histórica           |
| Arquivos inválidos   | quarantine                 |
| Arquivos processados | archive                    |
| Identificação        | SHA-256                    |
| Linhagem             | arquivo, linha e batch     |
| Deduplicação         | `row_id`                   |
| Armazenamento        | Delta Lake                 |
| Escrita              | `MERGE` incremental        |
| Silver               | processamento por partição |
| Dados atrasados      | late-arriving data         |
| Gold                 | atualização seletiva       |
| Consumo              | FastAPI                    |
| Rotas                | GeoJSON                    |
| Validação            | testes automatizados       |

---

# Limitações atuais

A implementação foi construída principalmente para execução local e validação arquitetural.

Ainda podem ser evoluídos:

```text
Local filesystem
        ↓
Object Storage

Batch por arquivos
        ↓
Streaming / filas / CDC

Processamento local
        ↓
Engine distribuído

Configuração local
        ↓
Configuração por ambiente
```

Também são evoluções naturais:

* paginação na API;
* predicate pushdown;
* autenticação e autorização;
* observabilidade;
* logs estruturados;
* métricas e alertas;
* mais testes de integração;
* documentação formal do protocolo;
* padronização de unidades e timezone.

---

# Possível evolução

```text
              Local Lakehouse
                     │
                     ▼
               Object Storage
                     │
                     ▼
                Data Catalog
                     │
                     ▼
          Processamento distribuído
                     │
                     ▼
                Orquestração
                     │
                     ▼
              Observabilidade
                     │
                     ▼
          Lakehouse corporativo
```

Mesmo com uma infraestrutura diferente, os princípios permanecem:

```text
linhagem
   +
idempotência
   +
qualidade
   +
controle
   +
processamento incremental
```

---

# Resumo

```text
CSV
 │
 ▼
Validação
 │
 ▼
Controle
 │
 ▼
Bronze
 │
 ▼
Silver
 │
 ▼
Gold
 │
 ▼
API
```

O projeto implementa um fluxo completo de engenharia de dados capaz de:

```text
✓ receber
✓ validar
✓ rastrear
✓ preservar
✓ tratar
✓ consolidar
✓ atualizar incrementalmente
✓ disponibilizar
```

dados provenientes de rastreadores.

A implementação serve como uma base prática para compreender como um Lakehouse pode evoluir de uma simples ingestão de arquivos para uma arquitetura incremental, rastreável e preparada para integração com outros sistemas.
