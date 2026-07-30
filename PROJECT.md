# Documentação técnica e operacional do Local Lakehouse Experiment
 
**Data da análise:** 28/07/2026  
**Escopo principal:** fluxo Data Lakehouse em Delta Lake, suas camadas Bronze, Silver e Gold, orquestração e API de consumo.  
**Escopo complementar:** comparação com o Data Lake em Parquet e análise crítica do projeto.

> Este documento foi escrito para permitir que uma pessoa que não participou do desenvolvimento consiga instalar, executar, validar, depurar e evoluir o projeto. A explicação acompanha o comportamento real do código disponível no repositório na data indicada.

## Sumário

1. Visão executiva
2. Conceitos fundamentais
3. Estrutura do repositório
4. Fluxo de dados ponta a ponta
5. Preparação do ambiente
6. Contrato do arquivo de entrada
7. Camada Raw
8. Camada Bronze do Lakehouse
9. Camada Silver do Lakehouse
10. Camada Gold do Lakehouse
11. Orquestrador do pipeline
12. Camada de leitura da API
13. API REST FastAPI
14. Fluxo Data Lake em Parquet
15. Execução passo a passo
16. Validação dos resultados
17. Regras de negócio consolidadas
18. Tratamento de erros e troubleshooting
19. Limitações e inconsistências atuais
20. Roadmap técnico recomendado
21. Glossário
22. Apêndices

# 1. Visão executiva

O projeto demonstra duas implementações paralelas da arquitetura medalhão:

- **Data Lake:** persiste arquivos Parquet puros em `data/lake`.
- **Data Lakehouse:** persiste tabelas Delta em `data/lakehouse`.

O fluxo principal do Lakehouse é:

```text
CSV de rastreador
    ↓
Raw: arquivo de origem
    ↓
Bronze: cópia integral em Delta Table
    ↓
Silver: normalização, tipagem, separação por domínio e rejeições
    ↓
Gold: deduplicação e produtos analíticos
    ↓
API REST: leitura controlada da Gold
    ↓
Dashboard, BI, mapa ou outro consumidor
```

A arquitetura é adequada como prova de conceito local porque combina:

- `pandas` para ingestão inicial do CSV;
- `deltalake` para leitura e escrita das Delta Tables;
- `DuckDB` para executar SQL analítico local sobre datasets Arrow/Delta;
- `PyArrow` como ponte de dados colunar;
- `FastAPI` e `Pydantic` para expor produtos Gold por HTTP;
- `uv` para ambiente e dependências.

O pipeline atual trabalha como **full refresh**. Cada execução sobrescreve as saídas, em vez de anexar somente dados novos. Isso torna a execução simples e reprodutível para estudo, mas ainda não representa uma ingestão incremental ou streaming de produção.

## 1.1 Produtos gerados

A execução completa do Lakehouse produz os seguintes datasets:

| Camada | Tabela ou diretório | Granularidade | Finalidade |
| --- | --- | --- | --- |
| Bronze | logs_rastreador_2026-07-01 | Uma linha por linha do CSV | Preservar a ingestão original em formato Delta. |
| Silver | telemetry_events | Uma linha por mensagem de telemetria estruturalmente aceita | Eventos tipados e classificados. |
| Silver | device_identity_events | Uma linha por mensagem T1 aceita | Histórico de identidade ICCID/IMSI/IMEI. |
| Silver | rejected_logs | Uma linha por registro estruturalmente rejeitado | Auditoria de qualidade e investigação. |
| Gold | dim_device | Uma linha por dispositivo | Visão dimensional e identidade atual. |
| Gold | device_last_position | Uma linha por dispositivo | Última posição GPS aproveitável. |
| Gold | device_route_points | Uma linha por ponto GPS válido, dispositivo e data | Trajeto cronológico para mapas. |
| Gold | device_daily_summary | Uma linha por dispositivo e data | Indicadores operacionais e de telemetria. |
| Gold | data_quality_summary | Uma linha por data de métrica | Indicadores de aceitação, rejeição e causas. |

# 2. Conceitos fundamentais

## 2.1 Raw, Bronze, Silver e Gold

**Raw** é a zona de chegada. O dado ainda está no formato de origem, sem transformação. No projeto, é o CSV colocado em `data/raw`.

**Bronze** é a cópia persistida do dado ingerido. A intenção é manter máxima fidelidade ao arquivo de entrada. No Lakehouse, essa cópia passa a ser uma Delta Table, mas o conteúdo não é limpo nem reorganizado.

**Silver** transforma o dado em estruturas confiáveis para processamento posterior. No projeto, a Silver:

- renomeia colunas;
- remove espaços nas pontas;
- transforma strings vazias em `NULL`;
- tenta converter timestamps e números;
- separa telemetria de identidade;
- registra rejeições estruturais;
- calcula indicadores de qualidade de posição.

**Gold** entrega produtos de consumo. Ela reduz redundância, escolhe registros atuais, agrega dados e organiza tabelas orientadas a consultas, API e dashboard.

## 2.2 Delta Table

Uma Delta Table é um diretório que contém arquivos Parquet e um diretório `_delta_log`. Os arquivos Parquet armazenam os dados; o log registra versões e operações da tabela. Por isso, o código valida tanto a existência do diretório quanto a existência de `_delta_log`.

## 2.3 Granularidade

Granularidade, ou *grain*, define o que uma linha representa. Saber a granularidade evita somas e junções incorretas. Por exemplo:

- em `telemetry_events`, uma linha representa uma mensagem aceita;
- em `device_daily_summary`, uma linha representa um dispositivo em um dia;
- em `dim_device`, uma linha representa um dispositivo, independentemente do número de eventos.

## 2.4 Full refresh e idempotência

O uso de `mode="overwrite"` faz cada camada reconstruir a saída. Com o mesmo arquivo de entrada e o mesmo código, a execução tende a produzir o mesmo estado final. Isso é útil para uma prova de conceito. Entretanto, sobrescrever não é o mesmo que implementar ingestão incremental, `MERGE`, controle de watermark ou processamento exatamente uma vez.

# 3. Estrutura do repositório

```text
local-lakehouse-experiment/
├── data/
│   ├── raw/                  # arquivos de entrada locais
│   ├── lake/                 # implementação em Parquet puro
│   │   ├── 01_bronze/
│   │   ├── 02_silver/
│   │   └── 03_gold/
│   └── lakehouse/            # implementação Delta Lake
│       ├── 01_bronze/
│       ├── 02_silver/
│       └── 03_gold/
├── notebook/                 # atualmente contém apenas .gitkeep
├── src/
│   ├── lake_01_bronze.py
│   ├── lake_02_silver.py
│   ├── lake_03_gold.py
│   ├── lake_pipeline.py
│   ├── lakehouse_01_bronze.py
│   ├── lakehouse_02_silver.py
│   ├── lakehouse_03_gold.py
│   ├── lakehouse_pipeline.py
│   └── api/
│       ├── __init__.py
│       ├── lakehouse_reader.py
│       └── main.py
├── .gitignore
├── README.md
├── lakehouse.excalidraw
├── pyproject.toml
└── uv.lock
```

## 3.1 Responsabilidade de cada arquivo Python

| Arquivo | Responsabilidade |
| --- | --- |
| src/lakehouse_01_bronze.py | Lê o CSV e cria a Delta Table Bronze. |
| src/lakehouse_02_silver.py | Normaliza a Bronze e cria telemetria, identidade e rejeições. |
| src/lakehouse_03_gold.py | Deduplica e cria as cinco tabelas Gold. |
| src/lakehouse_pipeline.py | Executa Bronze, Silver e Gold em sequência. |
| src/api/lakehouse_reader.py | Valida e lê as tabelas Gold; aplica filtros e normalizações. |
| src/api/main.py | Define modelos Pydantic e endpoints FastAPI. |
| src/lake_*.py | Versão equivalente baseada em Parquet puro. |
| src/lake_pipeline.py | Orquestra o fluxo Parquet. |

# 4. Fluxo de dados ponta a ponta

## 4.1 Sequência de execução

1. O CSV é colocado em `data/raw/logs_rastreador_2026-07-01.csv`.
2. A Bronze lê o arquivo inteiro com `pandas.read_csv`.
3. A Bronze grava uma Delta Table em `data/lakehouse/01_bronze/logs_rastreador_2026-07-01`.
4. A Silver abre o snapshot Bronze e o registra no DuckDB como relação `bronze`.
5. A Silver cria a view temporária `bronze_normalized`.
6. A Silver deriva três DataFrames: telemetria, identidade e rejeitados.
7. Cada DataFrame é salvo como Delta Table, com particionamento por data.
8. A Gold abre as três tabelas Silver e registra relações DuckDB.
9. A Gold cria bases deduplicadas temporárias.
10. A Gold gera cinco produtos analíticos.
11. A API abre as tabelas Gold diretamente pelo `deltalake` e converte os resultados para objetos Python.
12. O FastAPI valida as respostas e entrega JSON ou GeoJSON.

## 4.2 Dependências entre tabelas

```text
Bronze Delta
  └── bronze_normalized
       ├── telemetry_events ──┬── dim_device
       │                      ├── device_last_position
       │                      ├── device_route_points
       │                      └── device_daily_summary
       ├── device_identity_events ── dim_device
       └── rejected_logs ─────────── data_quality_summary

telemetry_events + device_identity_events + rejected_logs
  └── data_quality_summary
```

# 5. Preparação do ambiente

## 5.1 Pré-requisitos

- Git.
- `uv` instalado.
- Uma versão de Python compatível com `pyproject.toml`.
- O CSV de rastreadores com os nomes de colunas esperados.

## 5.2 Atenção à versão do Python

O `README.md` informa Python 3.13, porém o `pyproject.toml` exige `requires-python = ">=3.14"`. O gerenciador `uv` segue o `pyproject.toml`, portanto a exigência efetiva do projeto é Python 3.14 ou superior, salvo alteração do arquivo.

Antes de executar, confirme:

```powershell
uv --version
uv python list
```

Para instalar uma versão compatível pelo `uv`:

```powershell
uv python install 3.14
```

## 5.3 Clonagem e sincronização

```powershell
git clone https://github.com/Zuimbra/local-lakehouse-experiment.git
cd local-lakehouse-experiment
uv sync
```

`uv sync` lê `pyproject.toml` e `uv.lock`, cria ou atualiza `.venv` e instala as dependências resolvidas.

## 5.4 Dependências declaradas

| Dependência | Uso no projeto |
| --- | --- |
| deltalake >= 1.6.2 | Criação, validação e leitura das Delta Tables. |
| duckdb >= 1.5.4 | Execução de SQL analítico local. |
| fastapi >= 0.139.2 | API REST. |
| pandas >= 3.0.3 | Leitura inicial do CSV e DataFrames intermediários. |
| pyarrow >= 25.0.0 | Dataset colunar compartilhado entre Delta Lake e DuckDB. |
| uvicorn[standard] >= 0.51.0 | Servidor ASGI para executar a API. |

# 6. Contrato do arquivo de entrada

## 6.1 Local e nome exigidos pelo código atual

O script Bronze não recebe parâmetro. Ele procura exatamente:

```text
data/raw/logs_rastreador_2026-07-01.csv
```

Portanto, renomear o arquivo ou usar outra data sem alterar o código causa `FileNotFoundError`.

## 6.2 O arquivo não deve ser assumido como presente em um clone novo

O `.gitignore` ignora `data/raw/**/*`, com exceção de diretórios, `.gitkeep` e `sample_logs_rastreador.csv`. O nome exigido pelo pipeline não é a exceção declarada. Na prática, a pessoa que clonar o projeto deve colocar o arquivo manualmente ou adaptar o script para receber o nome como argumento.

## 6.3 Cabeçalhos esperados

A Silver referencia diretamente os seguintes cabeçalhos. Se um deles não existir, a consulta SQL falha porque o DuckDB não encontra a coluna.

| Coluna CSV | Nome normalizado | Tratamento comum | Significado no pipeline |
| --- | --- | --- | --- |
| DATA_SERVIDOR | server_timestamp | TIMESTAMP por TRY_CAST | Data e hora em que o servidor recebeu ou registrou a mensagem. |
| TM_STAMP | device_timestamp | TIMESTAMP por TRY_CAST | Data e hora informada pelo próprio rastreador. |
| TIPO_LOG | log_type | VARCHAR limpo | Classificação original do registro no arquivo. |
| MESS_TYPE | message_type | VARCHAR limpo | Tipo lógico da mensagem, como T1, T2, T3 etc. |
| REPT_TYPE | report_type_raw | VARCHAR limpo | Tipo de reporte ainda sem tipagem numérica. |
| PRT_VER | protocol_version | VARCHAR limpo | Versão do protocolo do equipamento. |
| S/N ou IMEI | device_serial_raw | VARCHAR limpo | Identificador bruto do rastreador; pode começar com M. |
| TERM_STATUS | terminal_status | VARCHAR limpo | Estado do terminal conforme o protocolo. |
| BAT_VOLT | battery_voltage_raw | VARCHAR limpo | Tensão de bateria em telemetria; em T1 é reutilizado como ICCID. |
| LOC_STATUS | location_status_raw | VARCHAR limpo | Estado de localização em telemetria; em T1 é campo auxiliar de identidade. |
| LAT | latitude_raw | VARCHAR limpo | Latitude em telemetria; em T1 é reutilizada como IMSI. |
| LONT | longitude_raw | VARCHAR limpo | Longitude em telemetria; em T1 é reutilizada como IMEI. |
| SPEED | speed_raw | VARCHAR limpo | Velocidade bruta. |
| DIR | direction_raw | VARCHAR limpo | Direção ou rumo em graus. |
| INT_BATT | internal_battery_raw | VARCHAR limpo | Nível ou tensão da bateria interna. |
| ODO_TRIP | odometer_trip_raw | VARCHAR limpo | Odômetro parcial da viagem. |
| ODO_TOTAL | odometer_total_raw | VARCHAR limpo | Odômetro total acumulado. |
| HORIMETER | horimeter_raw | VARCHAR limpo | Horímetro acumulado. |
| HDOP | hdop_raw | VARCHAR limpo | Diluição horizontal de precisão do GPS. |
| MCC | mcc | VARCHAR limpo | Mobile Country Code da rede celular. |
| MNC | mnc | VARCHAR limpo | Mobile Network Code da operadora. |
| LAC | lac | VARCHAR limpo | Location Area Code da célula. |
| CELL_ID | cell_id | VARCHAR limpo | Identificador da célula de telefonia. |
| RX_LEVEL | rx_level_raw | VARCHAR limpo | Nível bruto do sinal recebido. |
| SER_COUNT | serial_count_raw | VARCHAR limpo | Contador serial da mensagem. |
| TX_TECH | transmission_technology | VARCHAR limpo | Tecnologia de transmissão usada pelo dispositivo. |
| GRP_MSG | message_group | VARCHAR limpo | Grupo da mensagem no protocolo. |
| IO_STATUS | io_status | VARCHAR limpo | Estado de entradas e saídas do rastreador. |
| DRIVER_ID | driver_id | VARCHAR limpo | Identificador do motorista quando disponível. |
| PASS_ID | passenger_id | VARCHAR limpo | Identificador do passageiro quando disponível. |
| RPM | rpm_raw | VARCHAR limpo | Rotação do motor. |
| TACHO_SPD | tachograph_speed_raw | VARCHAR limpo | Velocidade informada pelo tacógrafo. |
| TACHO_ODO | tachograph_odometer_raw | VARCHAR limpo | Odômetro informado pelo tacógrafo. |
| TEMP_1 | temperature_1_raw | VARCHAR limpo | Primeiro canal de temperatura. |
| TEMP_2 | temperature_2_raw | VARCHAR limpo | Segundo canal de temperatura. |
| TEMP_3 | temperature_3_raw | VARCHAR limpo | Terceiro canal de temperatura. |
| TEMP_4 | temperature_4_raw | VARCHAR limpo | Quarto canal de temperatura. |

## 6.4 Requisitos mínimos para uma linha ser aceita

Uma linha só entra em uma tabela tratada quando:

- `MESS_TYPE` existe e segue o padrão `T` mais um ou mais dígitos;
- pelo menos um entre `TM_STAMP` e `DATA_SERVIDOR` é convertível para timestamp;
- `S/N ou IMEI` não está vazio;
- para telemetria, o tipo não é `T1`;
- para identidade, o tipo é exatamente `T1`.

Os demais campos podem estar ausentes ou ser inválidos. Em muitos casos o `TRY_CAST` converte o valor inválido em `NULL`, mas a linha continua aceita.

# 7. Camada Raw

A camada Raw não possui script próprio. Ela é uma convenção de diretório para o arquivo recebido. Sua responsabilidade é manter o arquivo original disponível para ingestão.

## 7.1 Boas práticas ao usar a Raw

- não editar o CSV depois de recebido;
- registrar origem, data de recebimento e checksum;
- usar nomes estáveis e sem colisão;
- manter uma cópia original antes de qualquer correção manual;
- preferir uma pasta por fonte e por data quando o volume crescer.

No código atual, a Raw não recebe metadados de ingestão. O nome do arquivo é a única informação de origem recuperada indiretamente pela Silver.

# 8. Camada Bronze do Lakehouse

**Arquivo:** [`src/lakehouse_01_bronze.py`](https://github.com/Zuimbra/local-lakehouse-experiment/blob/main/src/lakehouse_01_bronze.py)

## 8.1 Objetivo

Ler o CSV sem aplicar regra de negócio e persistir o conteúdo como Delta Table. A Bronze cria uma fronteira transacional entre o arquivo de entrada e as transformações posteriores.

## 8.2 Explicação completa dos imports

### `from pathlib import Path`

`Path` representa caminhos de forma orientada a objetos. O uso evita concatenar strings com `\` ou `/` e melhora a portabilidade entre Windows, Linux e macOS.

### `import pandas as pd`

O `pandas` é usado somente para `pd.read_csv`. O resultado é um DataFrame em memória com todas as linhas do arquivo.

### `from deltalake import write_deltalake`

`write_deltalake` grava o DataFrame como uma Delta Table. A função cria arquivos Parquet e o diretório `_delta_log`.

## 8.3 Função `load_bronze_data`

### 8.3.1 Descoberta da raiz do projeto

```python
project_dir = Path(__file__).resolve().parent.parent
```

A expressão deve ser lida de dentro para fora:

1. `__file__` é o caminho do próprio script.
2. `Path(__file__)` transforma a string em objeto `Path`.
3. `resolve()` produz caminho absoluto e resolve referências como `..`.
4. primeiro `.parent` aponta para `src`;
5. segundo `.parent` aponta para a raiz do repositório.

Essa estratégia permite executar o script a partir de diretórios diferentes sem depender do diretório atual do terminal.

### 8.3.2 Construção de `raw_path`

O operador `/` de `Path` concatena partes do caminho. O resultado final é o arquivo CSV esperado.

### 8.3.3 Construção de `bronze_path`

A saída é um diretório, não um arquivo único:

```text
data/lakehouse/01_bronze/logs_rastreador_2026-07-01/
```

Dentro dele haverá `_delta_log` e um ou mais arquivos Parquet.

### 8.3.4 Log inicial

O `print` mostra a origem que será lida. Isso é útil para diagnóstico, mas ainda não substitui logging estruturado.

### 8.3.5 Validação do arquivo

`raw_path.exists()` evita uma mensagem obscura do `pandas`. Quando o arquivo não existe, o script interrompe imediatamente com `FileNotFoundError`.

### 8.3.6 Leitura do CSV

`pd.read_csv(raw_path)`:

- infere tipos automaticamente;
- carrega todo o arquivo em memória;
- usa vírgula como separador padrão;
- usa UTF-8 por padrão quando possível;
- pode falhar por encoding, delimitador, cabeçalho, linha malformada ou memória insuficiente.

A Bronze atual não especifica `dtype`, `encoding`, `sep`, `on_bad_lines` ou chunks. Isso significa que o comportamento depende da inferência do pandas.

### 8.3.7 Criação do diretório pai

`bronze_path.parent.mkdir(parents=True, exist_ok=True)` cria `data/lakehouse/01_bronze`, incluindo pais ausentes. O diretório final da tabela é criado pelo escritor Delta.

### 8.3.8 Escrita Delta

```python
write_deltalake(bronze_path, df, mode="overwrite")
```

- `bronze_path`: destino da tabela;
- `df`: dados lidos;
- `mode="overwrite"`: substitui o estado anterior da tabela.

A execução não faz append. Se o arquivo tiver 1.000 linhas hoje e 900 amanhã, a Bronze final terá somente as 900 linhas da última execução.

### 8.3.9 Tratamento de exceções

Qualquer exceção dentro do bloco `try` é transformada em `RuntimeError`. O `from e` preserva a exceção original como causa, o que mantém o traceback encadeado.

A verificação de inexistência do arquivo ocorre antes do `try`, portanto o `FileNotFoundError` não é embrulhado em `RuntimeError`.

### 8.3.10 Bloco de execução direta

```python
if __name__ == "__main__":
    load_bronze_data()
```

Quando o arquivo é executado diretamente, a função roda. Quando é importado pelo orquestrador, a função não roda automaticamente; o orquestrador decide quando chamá-la.

## 8.4 Entrada e saída da Bronze

| Item | Descrição |
| --- | --- |
| Entrada | CSV completo em data/raw/logs_rastreador_2026-07-01.csv. |
| Transformação | Nenhuma regra de negócio; somente leitura e mudança de formato. |
| Saída | Delta Table em data/lakehouse/01_bronze/logs_rastreador_2026-07-01. |
| Modo | Overwrite/full refresh. |
| Particionamento | Nenhum. |
| Metadados adicionais | Nenhum; não grava ingestion_timestamp nem source_file. |

## 8.5 Importância da Bronze

A Bronze desacopla a origem CSV do restante do pipeline. Depois que a tabela foi criada, a Silver lê uma fonte colunar e transacional. Em um desenho de produção, a Bronze também seria o local para registrar metadados técnicos, controlar schema drift e manter histórico imutável de ingestões.

# 9. Camada Silver do Lakehouse

**Arquivo:** [`src/lakehouse_02_silver.py`](https://github.com/Zuimbra/local-lakehouse-experiment/blob/main/src/lakehouse_02_silver.py)

## 9.1 Objetivo

Transformar a Bronze em três domínios:

1. telemetria estruturalmente aceita;
2. eventos de identidade T1;
3. linhas rejeitadas com motivo.

A Silver preserva o histórico de eventos. Ela não deduplica e não escolhe uma única identidade por dispositivo. Essas operações ficam para a Gold.

## 9.2 Imports

### `Path`

Usado para localizar raiz, entrada Bronze e saídas Silver.

### `duckdb`

Cria um banco em memória e executa SQL sobre o dataset Arrow derivado da Delta Table.

### `DeltaTable`

Abre a tabela Bronze e expõe seu snapshot atual.

### `write_deltalake`

Grava os três DataFrames resultantes como Delta Tables.

## 9.3 Caminhos definidos

- `bronze_path`: tabela criada pela Bronze;
- `silver_path`: raiz da camada Silver;
- `telemetry_path`: saída `telemetry_events`;
- `identity_path`: saída `device_identity_events`;
- `rejected_path`: saída `rejected_logs`.

`silver_path.mkdir(parents=True, exist_ok=True)` garante a existência da pasta da camada.

## 9.4 Validação da entrada

O script faz duas validações distintas:

1. `bronze_path.is_dir()` confirma que o diretório existe;
2. `(bronze_path / "_delta_log").is_dir()` confirma a assinatura mínima de uma Delta Table.

A primeira falha gera `FileNotFoundError`. A segunda gera `ValueError`, porque o caminho existe, mas não representa a estrutura esperada.

## 9.5 Conexão DuckDB e registro da Bronze

`duckdb.connect()` sem caminho cria uma conexão temporária em memória. O código abre a Delta Table, converte-a em dataset PyArrow e registra esse dataset no DuckDB com o nome `bronze`.

A sequência é importante:

```text
DeltaTable → PyArrow Dataset → DuckDB relation
```

Isso permite usar SQL sem exportar a tabela para outro arquivo.

## 9.6 Construção de `source_file`

A Bronze não possui coluna de origem. A Silver cria:

```text
logs_rastreador_2026-07-01.csv
```

usando o nome do diretório Bronze e acrescentando `.csv`. O `replace("'", "''")` escapa aspas simples para evitar quebrar a string SQL.

Essa origem é **sintética**. Ela não prova qual arquivo físico gerou uma linha caso a Bronze venha a combinar várias fontes no futuro.

## 9.7 View temporária `bronze_normalized`

A view é a base compartilhada das três saídas. Ela não grava dados por si só. Sua função é evitar repetir a mesma limpeza em cada consulta.

### 9.7.1 Funções SQL usadas

- `CAST(... AS VARCHAR)`: converte o valor para texto antes da limpeza.
- `TRIM(...)`: remove espaços no início e no fim.
- `NULLIF(valor, '')`: transforma string vazia em `NULL`.
- `TRY_CAST(... AS TIMESTAMP)`: tenta converter; se falhar, retorna `NULL` em vez de abortar toda a consulta.
- alias `AS nome`: padroniza nomes em inglês e snake_case.

### 9.7.2 Por que BAT_VOLT, LAT e LONT ficam como texto

O protocolo reutiliza posições do arquivo conforme o tipo da mensagem. Em telemetria, esses campos representam tensão e coordenadas. Em T1, representam ICCID, IMSI e IMEI. Converter antes de saber o tipo poderia destruir zeros à esquerda ou ultrapassar precisão numérica. Por isso a conversão numérica ocorre somente no ramo de telemetria.

### 9.7.3 Dicionário completo da view comum

| Origem | Campo na view | Tipo/limpeza | Papel |
| --- | --- | --- | --- |
| DATA_SERVIDOR | server_timestamp | TIMESTAMP por TRY_CAST | Data e hora em que o servidor recebeu ou registrou a mensagem. |
| TM_STAMP | device_timestamp | TIMESTAMP por TRY_CAST | Data e hora informada pelo próprio rastreador. |
| TIPO_LOG | log_type | VARCHAR limpo | Classificação original do registro no arquivo. |
| MESS_TYPE | message_type | VARCHAR limpo | Tipo lógico da mensagem, como T1, T2, T3 etc. |
| REPT_TYPE | report_type_raw | VARCHAR limpo | Tipo de reporte ainda sem tipagem numérica. |
| PRT_VER | protocol_version | VARCHAR limpo | Versão do protocolo do equipamento. |
| S/N ou IMEI | device_serial_raw | VARCHAR limpo | Identificador bruto do rastreador; pode começar com M. |
| TERM_STATUS | terminal_status | VARCHAR limpo | Estado do terminal conforme o protocolo. |
| BAT_VOLT | battery_voltage_raw | VARCHAR limpo | Tensão de bateria em telemetria; em T1 é reutilizado como ICCID. |
| LOC_STATUS | location_status_raw | VARCHAR limpo | Estado de localização em telemetria; em T1 é campo auxiliar de identidade. |
| LAT | latitude_raw | VARCHAR limpo | Latitude em telemetria; em T1 é reutilizada como IMSI. |
| LONT | longitude_raw | VARCHAR limpo | Longitude em telemetria; em T1 é reutilizada como IMEI. |
| SPEED | speed_raw | VARCHAR limpo | Velocidade bruta. |
| DIR | direction_raw | VARCHAR limpo | Direção ou rumo em graus. |
| INT_BATT | internal_battery_raw | VARCHAR limpo | Nível ou tensão da bateria interna. |
| ODO_TRIP | odometer_trip_raw | VARCHAR limpo | Odômetro parcial da viagem. |
| ODO_TOTAL | odometer_total_raw | VARCHAR limpo | Odômetro total acumulado. |
| HORIMETER | horimeter_raw | VARCHAR limpo | Horímetro acumulado. |
| HDOP | hdop_raw | VARCHAR limpo | Diluição horizontal de precisão do GPS. |
| MCC | mcc | VARCHAR limpo | Mobile Country Code da rede celular. |
| MNC | mnc | VARCHAR limpo | Mobile Network Code da operadora. |
| LAC | lac | VARCHAR limpo | Location Area Code da célula. |
| CELL_ID | cell_id | VARCHAR limpo | Identificador da célula de telefonia. |
| RX_LEVEL | rx_level_raw | VARCHAR limpo | Nível bruto do sinal recebido. |
| SER_COUNT | serial_count_raw | VARCHAR limpo | Contador serial da mensagem. |
| TX_TECH | transmission_technology | VARCHAR limpo | Tecnologia de transmissão usada pelo dispositivo. |
| GRP_MSG | message_group | VARCHAR limpo | Grupo da mensagem no protocolo. |
| IO_STATUS | io_status | VARCHAR limpo | Estado de entradas e saídas do rastreador. |
| DRIVER_ID | driver_id | VARCHAR limpo | Identificador do motorista quando disponível. |
| PASS_ID | passenger_id | VARCHAR limpo | Identificador do passageiro quando disponível. |
| RPM | rpm_raw | VARCHAR limpo | Rotação do motor. |
| TACHO_SPD | tachograph_speed_raw | VARCHAR limpo | Velocidade informada pelo tacógrafo. |
| TACHO_ODO | tachograph_odometer_raw | VARCHAR limpo | Odômetro informado pelo tacógrafo. |
| TEMP_1 | temperature_1_raw | VARCHAR limpo | Primeiro canal de temperatura. |
| TEMP_2 | temperature_2_raw | VARCHAR limpo | Segundo canal de temperatura. |
| TEMP_3 | temperature_3_raw | VARCHAR limpo | Terceiro canal de temperatura. |
| TEMP_4 | temperature_4_raw | VARCHAR limpo | Quarto canal de temperatura. |

## 9.8 Saída `telemetry_events`

### 9.8.1 CTE `typed_telemetry`

A CTE é uma consulta nomeada usada apenas dentro da instrução atual. Ela converte campos de telemetria para tipos adequados e aplica os critérios estruturais de aceitação.

### 9.8.2 Escolha do timestamp do evento

```text
event_timestamp = COALESCE(device_timestamp, server_timestamp)
```

A prioridade é o horário do dispositivo. Se ele estiver ausente ou inválido, o horário do servidor é usado. A linha só é rejeitada quando ambos são `NULL`.

Consequências:

- dispositivo válido e servidor inválido: evento aceito;
- dispositivo inválido e servidor válido: evento aceito com horário do servidor;
- ambos inválidos ou ausentes: evento rejeitado;
- os dois válidos, porém divergentes: o horário do dispositivo prevalece.

### 9.8.3 Validação do tipo de mensagem

`regexp_full_match(message_type, '^T[0-9]+$')` exige que o texto inteiro seja `T` seguido de pelo menos um dígito. `T1` é removido do ramo de telemetria porque possui schema lógico de identidade.

### 9.8.4 Normalização do serial

`REGEXP_REPLACE(device_serial_raw, '^M', '')` remove somente um `M` maiúsculo no início. Não remove `m` minúsculo, espaços internos ou outros prefixos.

### 9.8.5 Conversões tolerantes

`TRY_CAST` evita que um valor ruim derrube a camada. Exemplo: velocidade `abc` resulta em `speed = NULL`. Isso não envia a linha para `rejected_logs`, porque a regra de rejeição atual é estrutural, não uma validação completa de cada medida.

O duplo cast de `report_type` e `serial_count` aceita representações como `"3.0"`: primeiro converte para `DOUBLE`, depois para inteiro.

### 9.8.6 Campos resultantes

| Campo | Tipo lógico | Interpretação |
| --- | --- | --- |
| event_date | DATE | Data derivada de event_timestamp; usada para particionamento. |
| server_timestamp | TIMESTAMP | Horário de recebimento no servidor. |
| device_timestamp | TIMESTAMP | Horário informado pelo rastreador. |
| event_timestamp | TIMESTAMP | COALESCE(device_timestamp, server_timestamp); horário de negócio do evento. |
| log_type | VARCHAR | Tipo de log normalizado. |
| message_type | VARCHAR | Tn válido, exceto T1. |
| report_type | INTEGER | report_type_raw convertido por DOUBLE e depois INTEGER. |
| protocol_version | VARCHAR | Versão do protocolo. |
| device_serial | VARCHAR | Serial sem um prefixo M inicial. |
| terminal_status | VARCHAR | Estado do terminal. |
| battery_voltage | DOUBLE | Tensão de bateria convertida; valor inválido vira NULL. |
| location_status | VARCHAR | Status de localização. |
| latitude | DOUBLE | Latitude convertida. |
| longitude | DOUBLE | Longitude convertida. |
| speed | DOUBLE | Velocidade convertida. |
| direction_degrees | DOUBLE | Direção convertida para graus. |
| internal_battery | DOUBLE | Bateria interna convertida. |
| odometer_trip | DOUBLE | Odômetro parcial convertido. |
| odometer_total | DOUBLE | Odômetro total convertido. |
| horimeter | DOUBLE | Horímetro convertido. |
| hdop | DOUBLE | Indicador de precisão GPS convertido. |
| mcc, mnc, lac, cell_id | VARCHAR | Identificadores de rede celular mantidos como texto. |
| rx_level | DOUBLE | Nível de recepção convertido. |
| serial_count | BIGINT | Contador convertido primeiro para DOUBLE e depois BIGINT. |
| transmission_technology | VARCHAR | Tecnologia de transmissão. |
| message_group | VARCHAR | Grupo da mensagem. |
| io_status | VARCHAR | Estado de I/O. |
| driver_id | VARCHAR | Identificador de motorista. |
| passenger_id | VARCHAR | Identificador de passageiro. |
| rpm | DOUBLE | Rotação do motor. |
| tachograph_speed | DOUBLE | Velocidade do tacógrafo. |
| tachograph_odometer | DOUBLE | Odômetro do tacógrafo. |
| temperature_1 ... temperature_4 | DOUBLE | Quatro canais de temperatura. |
| source_file | VARCHAR | Nome lógico do arquivo de origem sintetizado pela Silver. |
| has_valid_coordinates | BOOLEAN | Indica presença e faixa válida de latitude/longitude. |
| position_quality | VARCHAR | VALID, MISSING_COORDINATES, INVALID_COORDINATES ou LOW_GPS_PRECISION. |

### 9.8.7 Regras de qualidade de posição

| Situação | Condição | Resultado Silver | Observação |
| --- | --- | --- | --- |
| Coordenada ausente | latitude IS NULL ou longitude IS NULL | has_valid_coordinates = FALSE; position_quality = MISSING_COORDINATES | Registro permanece aceito na telemetria. |
| Coordenada fora da faixa | latitude fora de [-90, 90] ou longitude fora de [-180, 180] | FALSE; INVALID_COORDINATES | Registro permanece aceito na telemetria. |
| Baixa precisão | coordenadas válidas e hdop > 5 | TRUE; LOW_GPS_PRECISION | É posição numericamente válida, porém com precisão ruim. |
| Posição válida | coordenadas presentes e dentro das faixas; hdop ausente ou <= 5 | TRUE; VALID | Pode alimentar produtos Gold. |
| Ponto 0,0 | latitude = 0 e longitude = 0 | Silver marca como válido; Gold exclui de posição e rota | Regra de negócio aplicada apenas na Gold. |

### 9.8.8 Persistência

A tabela é gravada com:

- `mode="overwrite"`;
- `partition_by=["event_date"]`.

O particionamento organiza os arquivos por data e pode reduzir leitura quando o mecanismo aplica poda de partição. O script atual, porém, materializa primeiro o resultado inteiro em um DataFrame pandas por `.df()`.

## 9.9 Saída `device_identity_events`

### 9.9.1 Por que T1 é separada

T1 usa posições de colunas com significado diferente da telemetria. Misturar esses registros em uma única tabela produziria colunas semanticamente ambíguas.

### 9.9.2 Critérios de aceitação

- `message_type = 'T1'`;
- pelo menos um timestamp válido;
- serial presente.

Formato inválido de ICCID, IMSI ou IMEI **não rejeita** a linha. O código preserva o evento e adiciona flags booleanas.

### 9.9.3 Mapeamento especial da T1

- `battery_voltage_raw → iccid`;
- `location_status_raw → identity_auxiliary`;
- `latitude_raw → imsi`;
- `longitude_raw → imei`.

Os valores permanecem texto para preservar zeros à esquerda e evitar perda de precisão.

### 9.9.4 Campos resultantes

| Campo | Tipo lógico | Interpretação |
| --- | --- | --- |
| event_date | DATE | Data do evento de identidade. |
| server_timestamp | TIMESTAMP | Horário do servidor. |
| device_timestamp | TIMESTAMP | Horário do equipamento. |
| event_timestamp | TIMESTAMP | Horário do equipamento, com fallback para o servidor. |
| message_type | VARCHAR | Sempre T1 para registros aceitos nesta tabela. |
| report_type | INTEGER | Tipo de reporte convertido. |
| protocol_version | VARCHAR | Versão do protocolo. |
| device_serial_raw | VARCHAR | Identificador bruto. |
| device_serial | VARCHAR | Identificador sem prefixo M inicial. |
| iccid | VARCHAR | Valor originalmente localizado em BAT_VOLT nas mensagens T1. |
| identity_auxiliary | VARCHAR | Valor originalmente localizado em LOC_STATUS nas mensagens T1. |
| imsi | VARCHAR | Valor originalmente localizado em LAT nas mensagens T1. |
| imei | VARCHAR | Valor originalmente localizado em LONT nas mensagens T1. |
| source_file | VARCHAR | Origem lógica. |
| has_valid_iccid_format | BOOLEAN | Verdadeiro apenas para 18 a 22 dígitos. |
| has_valid_imsi_format | BOOLEAN | Verdadeiro apenas para 14 a 16 dígitos. |
| has_valid_imei_format | BOOLEAN | Verdadeiro apenas para 15 dígitos. |

### 9.9.5 Validações de formato

- ICCID: 18 a 22 dígitos;
- IMSI: 14 a 16 dígitos;
- IMEI: exatamente 15 dígitos.

As expressões regulares verificam somente comprimento e caracteres numéricos. Elas não calculam dígito verificador nem confirmam existência junto à operadora.

### 9.9.6 Preservação do histórico

A Silver não agrupa por dispositivo. Se um rastreador enviar dez mensagens T1, as dez são mantidas. A identidade atual é escolhida depois na `dim_device`.

## 9.10 Saída `rejected_logs`

### 9.10.1 O que é rejeitado

A consulta seleciona linhas com pelo menos uma destas falhas:

- tipo de mensagem ausente;
- tipo fora do padrão Tn;
- ausência de timestamp válido;
- ausência de serial.

### 9.10.2 Precedência do motivo

Uma linha pode ter múltiplos problemas, mas recebe apenas um `rejection_reason`. O `CASE` é avaliado de cima para baixo.

| Prioridade | Condição | Motivo | Efeito |
| --- | --- | --- | --- |
| 1 | message_type é NULL | MISSING_MESSAGE_TYPE | Tem prioridade sobre todas as outras falhas. |
| 2 | message_type não corresponde a ^T[0-9]+$ | INVALID_MESSAGE_TYPE | Exemplos: X1, T, texto livre ou valor fora do padrão. |
| 3 | device_timestamp e server_timestamp são ambos NULL após TRY_CAST | MISSING_OR_INVALID_TIMESTAMP | Abrange timestamp ausente e texto impossível de converter. |
| 4 | device_serial_raw é NULL | MISSING_DEVICE_SERIAL | Só aparece se as regras anteriores não tiverem sido acionadas. |
| 5 | Linha entrou no filtro de rejeição sem casar com as regras anteriores | UNKNOWN_REJECTION_REASON | É uma salvaguarda; com o filtro atual tende a não ocorrer. |

### 9.10.3 `rejection_date`

O código tenta formatar `event_timestamp` como `YYYY-MM-DD`. Se não houver timestamp, grava `unknown`. A tabela é particionada por esse campo.

Uma mensagem sem timestamp, mas com tipo e serial válidos, será rejeitada com `MISSING_OR_INVALID_TIMESTAMP` e ficará na partição `rejection_date=unknown`.

### 9.10.4 O que não causa rejeição hoje

- velocidade inválida;
- bateria inválida;
- latitude ou longitude ausente;
- coordenada fora da faixa;
- HDOP alto;
- formato inválido de ICCID, IMSI ou IMEI;
- odômetro regressivo;
- serial com prefixo não esperado, desde que não vazio.

Essas condições geram `NULL`, flags ou tratamento posterior, mas não entram em `rejected_logs`.

## 9.11 Finalização e erros

O bloco `except Exception` converte falhas em `RuntimeError` com contexto Silver. O `finally` fecha a conexão DuckDB mesmo quando uma consulta falha.

Como as três tabelas são escritas em sequência, a execução não é transacional entre tabelas. Pode ocorrer estado parcial: telemetria escrita e identidade falhando, por exemplo. Uma evolução robusta deveria escrever em área temporária e publicar todas as saídas de forma coordenada.

# 10. Camada Gold do Lakehouse

**Arquivo:** [`src/lakehouse_03_gold.py`](https://github.com/Zuimbra/local-lakehouse-experiment/blob/main/src/lakehouse_03_gold.py)

## 10.1 Objetivo

Transformar eventos Silver em produtos deduplicados e orientados a consumo. A Gold não é apenas uma cópia limpa: ela define granularidades de negócio e calcula indicadores.

## 10.2 Imports

- `Path`: caminhos.
- `duckdb`: SQL analítico.
- `DeltaTable`: abertura e validação das entradas Silver.
- `write_deltalake`: gravação dos produtos Gold.

## 10.3 Função `load_delta_table`

A função centraliza três responsabilidades:

1. verificar se o diretório existe;
2. verificar se `_delta_log` existe;
3. tentar abrir `DeltaTable(str(path))`.

O parâmetro `table_name` é usado somente para produzir mensagens de erro mais claras. O retorno é um objeto `DeltaTable` pronto para registro no DuckDB.

## 10.4 Caminhos Gold

O código define cinco destinos:

- `dim_device`;
- `device_last_position`;
- `device_daily_summary`;
- `data_quality_summary`;
- `device_route_points`.

Antes de processar, abre obrigatoriamente as três entradas Silver. Se qualquer uma estiver ausente ou inválida, a Gold não inicia.

## 10.5 Registro das entradas no DuckDB

Cada `DeltaTable` é convertida em PyArrow Dataset e registrada como:

- `silver_telemetry`;
- `silver_identity`;
- `silver_rejected`.

Essas relações temporárias existem apenas durante a execução.

## 10.6 Base deduplicada de telemetria

A Silver preserva retransmissões. A Gold cria `telemetry_gold_base` com apenas uma linha por chave lógica.

### 10.6.1 Chave lógica de deduplicação

A partição da janela usa:

1. `device_serial`;
2. `event_timestamp`;
3. `message_type`;
4. `serial_count`, substituindo `NULL` por marcador textual;
5. `latitude`, com marcador para `NULL`;
6. `longitude`, com marcador para `NULL`;
7. `speed`, com marcador para `NULL`.

### 10.6.2 Registro mantido

`ROW_NUMBER` ordena por:

1. `server_timestamp DESC NULLS LAST`;
2. `source_file DESC NULLS LAST`.

A linha de número 1 é mantida. Em outras palavras, para duplicatas lógicas, vence a recebida mais recentemente pelo servidor.

### 10.6.3 Importância

Sem essa etapa, retransmissões poderiam inflar `message_count`, médias, contagem de movimento e quantidade de pontos de rota.

## 10.7 Base deduplicada de identidade

A chave usa:

- dispositivo;
- timestamp;
- IMEI;
- IMSI;
- ICCID.

O desempate também privilegia `server_timestamp` e `source_file` mais recentes.

## 10.8 Tabela `dim_device`

### 10.8.1 Granularidade

Uma linha por `device_serial`.

### 10.8.2 CTE `identity_summary`

Agrupa a base de identidade por dispositivo e calcula:

- primeira e última identidade;
- quantidade de eventos de identidade;
- IMEI, IMSI, ICCID e campo auxiliar mais recentes por `ARG_MAX`;
- protocolo mais recente;
- flags de formato associadas ao evento mais recente.

`ARG_MAX(valor, event_timestamp)` retorna o valor da linha cujo timestamp é máximo.

### 10.8.3 CTE `telemetry_summary`

Calcula primeira telemetria, última telemetria, contagem de eventos e versão de protocolo mais recente.

### 10.8.4 CTE `all_activity`

Une identidade e telemetria com `UNION ALL`, preservando todas as ocorrências para descobrir primeira e última atividade global.

### 10.8.5 CTE `activity_summary`

Agrupa a atividade e obtém `first_seen_at` e `last_seen_at`.

### 10.8.6 CTE `devices`

Usa `UNION`, não `UNION ALL`, para formar a lista única de seriais presentes em qualquer uma das duas bases.

### 10.8.7 Junções finais

`LEFT JOIN` mantém todos os dispositivos, mesmo que tenham somente telemetria ou somente identidade. Contagens ausentes são convertidas para zero por `COALESCE`.

### 10.8.8 Campos da dimensão

| Campo | Significado |
| --- | --- |
| device_serial | Chave natural do rastreador. |
| current_imei/current_imsi/current_iccid | Identidade mais recente conhecida. |
| current_identity_auxiliary | Campo auxiliar da T1 mais recente. |
| current_protocol_version | Protocolo da identidade mais recente; se ausente, da telemetria mais recente. |
| first_seen_at/last_seen_at | Primeira e última atividade em qualquer domínio. |
| first_identity_at/last_identity_at | Limites do histórico T1. |
| first_telemetry_at/last_telemetry_at | Limites do histórico de telemetria. |
| identity_event_count/telemetry_event_count | Contagens após deduplicação Gold. |
| has_identity_event/has_telemetry_event | Flags de presença. |
| current_*_format_valid | Validação de formato da identidade atual. |

A tabela é gravada sem partição porque sua granularidade é pequena e orientada a uma linha por dispositivo.

## 10.9 Tabela `device_last_position`

### 10.9.1 Granularidade

Uma linha por dispositivo que possua pelo menos uma posição aproveitável.

### 10.9.2 Filtros

- `has_valid_coordinates = TRUE`;
- exclusão explícita de `(0, 0)`.

O ponto 0,0 passa na validação numérica da Silver, mas é tratado como ausência prática de posição na Gold.

### 10.9.3 Escolha da última posição

A janela é particionada por dispositivo e ordenada por:

1. `event_timestamp DESC`;
2. `server_timestamp DESC NULLS LAST`;
3. `serial_count DESC NULLS LAST`.

### 10.9.4 Campos

A saída inclui data e horário da posição, horário de recebimento, coordenadas, velocidade, direção, bateria, odômetro, horímetro, HDOP, sinal, tipo de mensagem, contador, protocolo, qualidade e origem.

## 10.10 Tabela `device_route_points`

### 10.10.1 Granularidade

Uma linha por ponto GPS válido de um dispositivo em uma data.

### 10.10.2 CTE `valid_points`

Seleciona a base de telemetria deduplicada, exige coordenadas válidas e remove 0,0.

### 10.10.3 CTE `ordered_points`

Cria `point_sequence` com `ROW_NUMBER`, particionando por dispositivo e data e ordenando por:

1. horário do evento;
2. horário de recebimento;
3. contador serial.

Essa sequência é a ordem que a API usa para montar a LineString.

### 10.10.4 Campo `is_moving`

`COALESCE(speed, 0) >= 5` considera o ponto em movimento a partir de velocidade 5. Velocidade nula vira zero e, portanto, é classificada como não movimento.

### 10.10.5 Limites funcionais

A tabela representa a sequência observada de pontos, mas não:

- separa viagens;
- identifica ignição;
- remove saltos por distância ou velocidade impossível;
- interpola lacunas;
- aplica map matching em ruas;
- calcula distância geodésica.

## 10.11 Tabela `device_daily_summary`

### 10.11.1 Granularidade

Uma linha por `event_date` e `device_serial`.

### 10.11.2 Métricas calculadas

| Grupo | Campos | Regra |
| --- | --- | --- |
| Janela temporal | first_event_at, last_event_at | MIN e MAX do timestamp. |
| Volume | message_count | COUNT de eventos deduplicados. |
| Diversidade | distinct_message_type_count | COUNT DISTINCT de message_type. |
| Posição | valid_position_count | Contagem com has_valid_coordinates = TRUE. |
| Posição | invalid_position_count | Contagem em que has_valid_coordinates não é TRUE. |
| Precisão | low_gps_precision_count | Contagem de position_quality = LOW_GPS_PRECISION. |
| Qualidade | valid_position_percentage | valid_position_count / message_count × 100. |
| Movimento | moving_event_count | speed >= 5. |
| Parado | stopped_event_count | speed não nula e < 5. |
| Velocidade | average_speed | Média de toda velocidade não nula. |
| Velocidade | average_speed_while_moving | Média somente quando speed >= 5. |
| Velocidade | maximum_speed | Máximo do dia. |
| GPS | average/minimum/maximum_hdop | Estatísticas de HDOP. |
| Bateria externa | minimum/maximum/average_battery_voltage | Estatísticas de BAT_VOLT convertido. |
| Bateria interna | minimum/maximum/average_internal_battery | Estatísticas de INT_BATT. |
| Odômetro | first_odometer_total, last_odometer_total | ARG_MIN e ARG_MAX pelo timestamp. |
| Odômetro | odometer_delta_raw | Último menos primeiro, salvo regressão ou ausência. |
| Odômetro | has_odometer_regression | Verdadeiro quando o último é menor que o primeiro. |
| Rota resumida | first/last_valid_position_at | Primeiro e último timestamp com coordenada numericamente válida. |
| Rota resumida | first/last latitude/longitude | Coordenadas associadas aos extremos temporais. |

### 10.11.3 Nuances importantes

- `LOW_GPS_PRECISION` continua contando como posição válida porque as coordenadas estão dentro das faixas.
- `stopped_event_count` não inclui velocidade nula.
- `valid_position_count` na síntese diária inclui 0,0, pois o filtro diário verifica apenas `has_valid_coordinates`. Portanto, a regra de exclusão 0,0 aplicada em rota e última posição não é aplicada nesse indicador.
- o delta do odômetro é chamado `raw` porque a unidade do protocolo ainda não foi confirmada.
- em regressão de odômetro, o delta é `NULL` e a flag é verdadeira.

## 10.12 Tabela `data_quality_summary`

### 10.12.1 Granularidade

Uma linha por `metric_date`, incluindo a categoria `unknown` para rejeições sem data.

### 10.12.2 CTEs

- `telemetry_counts`: conta telemetria Silver por data;
- `identity_counts`: conta identidade Silver por data;
- `rejected_counts`: conta rejeições e cada motivo;
- `all_dates`: união das datas existentes em qualquer domínio;
- `combined`: junta as contagens e substitui ausências por zero.

### 10.12.3 Fórmulas

```text
accepted_event_count = telemetry_event_count + identity_event_count

total_event_count = accepted_event_count + rejected_event_count

rejection_percentage = rejected_event_count / total_event_count × 100
```

`NULLIF(total, 0)` evita divisão por zero. O percentual é arredondado para quatro casas.

### 10.12.4 Deduplicação e qualidade medem coisas diferentes

A qualidade usa as tabelas Silver originais, não as bases deduplicadas Gold. Assim, retransmissões aceitas contam como eventos aceitos no indicador de ingestão. Já os indicadores de negócio da Gold usam telemetria deduplicada. Essa diferença é coerente se a métrica de qualidade pretende medir linhas recebidas, mas deve estar documentada para evitar comparações incorretas.

## 10.13 Escrita e tratamento de erros

Todas as tabelas são gravadas com `mode="overwrite"` e `schema_mode="overwrite"`. As tabelas por data usam `partition_by`.

O `except` captura especificamente `duckdb.Error`. Falhas da escrita Delta que não forem subclasses de `duckdb.Error` podem propagar sem a mensagem personalizada. O `finally` fecha a conexão.

# 11. Orquestrador do pipeline

**Arquivo:** [`src/lakehouse_pipeline.py`](https://github.com/Zuimbra/local-lakehouse-experiment/blob/main/src/lakehouse_pipeline.py)

O arquivo importa as três funções e as chama na ordem correta:

```text
load_bronze_data()
load_silver_data()
load_gold_data()
```

## 11.1 Por que a ordem importa

- Silver depende da Bronze válida;
- Gold depende das três tabelas Silver;
- inverter ou pular uma etapa pode usar dados antigos ou causar falha de entrada ausente.

## 11.2 Comportamento em falha

Não há `try` no orquestrador. Se uma etapa falhar, a exceção interrompe o processo e as etapas posteriores não executam. As saídas de etapas já concluídas permanecem no disco.

## 11.3 Execução direta versus importação

O bloco `if __name__ == "__main__"` impede execução automática quando o módulo é importado em testes ou outro programa. A orquestração só ocorre ao executar o arquivo como script.

# 12. Camada de leitura da API

**Arquivo:** [`src/api/lakehouse_reader.py`](https://github.com/Zuimbra/local-lakehouse-experiment/blob/main/src/api/lakehouse_reader.py)

## 12.1 Objetivo

Separar acesso a dados da definição HTTP. O módulo sabe localizar, validar, abrir, ler e filtrar tabelas Gold. O `main.py` usa essas funções sem manipular diretamente arquivos Delta.

## 12.2 Constantes

### `PROJECT_ROOT`

`Path(__file__).resolve().parents[2]` sobe de `src/api/lakehouse_reader.py` para a raiz do projeto.

### `GOLD_DIR`

Aponta para `data/lakehouse/03_gold`.

### `GOLD_TABLES`

Lista branca das cinco tabelas reconhecidas. Essa lista impede que um nome arbitrário vindo de código externo seja transformado em caminho de arquivo.

### `QUALITY_COUNT_COLUMNS`

Centraliza os campos aditivos usados ao reagrupar métricas de qualidade.

## 12.3 Funções de infraestrutura

### `get_gold_table_path`

Valida o nome contra `GOLD_TABLES` e retorna o caminho. Nome desconhecido gera `ValueError` com as opções disponíveis.

### `validate_gold_table`

Confirma diretório e usa `DeltaTable.is_deltatable` para validação formal.

### `open_gold_table`

Abre o snapshot atual da tabela validada.

### `describe_gold_table`

Retorna:

- nome;
- caminho;
- versão Delta;
- quantidade de arquivos ativos;
- colunas com nome, tipo e nulabilidade.

Essa função alimenta o endpoint `/ready`.

### `read_gold_table`

Converte a tabela em PyArrow Dataset, materializa opcionalmente uma seleção de colunas e devolve `list[dict]`.

A leitura é integral. Para o MVP pequeno é simples, mas em grande volume deveria usar filtros pushdown, paginação, DuckDB ou outra camada de consulta.

## 12.4 Normalização de qualidade

### `normalize_metric_date`

Aceita `datetime`, `date` ou texto ISO. Valores vazios, nulos ou inválidos retornam `unknown`.

### `list_data_quality`

O fluxo é:

1. lê apenas colunas relevantes;
2. normaliza `metric_date`;
3. agrupa linhas que representem a mesma data em formatos diferentes;
4. soma colunas de contagem;
5. aplica `date_from` e `date_to` somente a datas conhecidas;
6. recalcula total e percentual;
7. ordena datas decrescentes e coloca `unknown` no final.

O reagrupamento protege a API contra históricos em que a partição/data possa aparecer como `2026-07-01` ou `2026-07-01 00:00:00`.

### `get_data_quality`

Normaliza o parâmetro, chama a listagem e retorna a primeira linha correspondente ou `None`.

## 12.5 Leitura de resumo diário

`DAILY_SUMMARY_COLUMNS` define explicitamente o contrato lido. `normalize_event_date` converte valores para `date` e descarta valores inválidos.

`list_daily_summaries`:

- lê a tabela;
- filtra opcionalmente por serial e período;
- converte event_date;
- ordena por data e serial em ordem decrescente.

## 12.6 Leitura de rotas

`ROUTE_POINT_COLUMNS` define os campos de rota.

### `list_route_devices`

Chama `list_route_points` para a data, extrai seriais únicos e ordena.

### `list_route_points`

- normaliza o serial informado;
- filtra por data e serial;
- exige latitude e longitude;
- converte para `float`;
- valida faixas;
- remove 0,0 novamente como defesa;
- ordena por serial, sequência e timestamp.

A validação repetida na API é útil como proteção de fronteira, mesmo que a Gold já tenha filtrado os pontos.

## 12.7 Bloco de teste manual

Quando executado diretamente, o módulo testa a data fixa `2026-07-01`, imprime dispositivos, quantidade de pontos, primeiro e último ponto. Esse bloco é diagnóstico manual, não teste automatizado.

# 13. API REST FastAPI

**Arquivo:** [`src/api/main.py`](https://github.com/Zuimbra/local-lakehouse-experiment/blob/main/src/api/main.py)

## 13.1 Criação da aplicação

`FastAPI` recebe título, descrição e versão `0.5.0`. Esses metadados aparecem no OpenAPI e no Swagger em `/docs`.

## 13.2 Imports principais

- `date` e `datetime`: tipagem e serialização temporal;
- `Any` e `Literal`: contratos flexíveis e valores fixos;
- `FastAPI`: aplicação;
- `HTTPException`: respostas de erro controladas;
- `Query`: documentação e validação de query parameters;
- `Response`: alteração manual do status em readiness;
- `status`: constantes HTTP;
- `BaseModel` e `Field`: modelos Pydantic e restrições.

## 13.3 Modelos de qualidade

`DataQualityRecord` exige contagens não negativas. `rejection_percentage` deve ficar entre 0 e 100. `DataQualityListResponse` encapsula `count` e `items`.

## 13.4 Modelos de resumo diário

`DailySummaryRecord` replica o contrato Gold. Campos que podem não existir são opcionais. Contagens são não negativas. Percentual de posição válida é limitado a 0–100.

`DailySummaryListResponse` encapsula a lista.

## 13.5 Modelos GeoJSON

`GeoJsonFeature` fixa `type="Feature"`. `RouteGeoJsonResponse` fixa `type="FeatureCollection"`. `properties` e `geometry` são dicionários para acomodar a estrutura GeoJSON.

## 13.6 Endpoint raiz `/`

Não aparece no schema por `include_in_schema=False`. Retorna nome, versão e atalhos para saúde, readiness, documentação e recursos principais.

## 13.7 Endpoint `/health`

Responde `{"status": "ok"}`. Ele confirma que o processo FastAPI está vivo, mas não verifica dados.

## 13.8 Endpoint `/ready`

Percorre `GOLD_TABLES` e chama `describe_gold_table`.

- todas disponíveis: status lógico `ready` e HTTP 200;
- alguma indisponível: status lógico `not_ready` e HTTP 503.

Para cada tabela, retorna versão Delta e quantidade de arquivos ativos ou uma mensagem de erro.

## 13.9 Endpoints de qualidade

### `GET /api/v1/data-quality`

Parâmetros opcionais:

- `date_from`;
- `date_to`.

Se início for posterior ao fim, responde 422. Caso contrário, retorna contagem e itens validados por Pydantic.

### `GET /api/v1/data-quality/{metric_date}`

Aceita `YYYY-MM-DD` ou `unknown`. Formato inválido gera 422. Ausência de registro gera 404.

## 13.10 Endpoints de resumo diário

### `GET /api/v1/daily-summary`

Filtros opcionais por período e `device_serial`. Serial vazio é normalizado para ausência.

### `GET /api/v1/daily-summary/{event_date}`

Busca os resumos da data. Regras:

- nenhum registro: 404;
- serial informado sem correspondência: 404;
- mais de um dispositivo e serial omitido: 409;
- um único resultado: retorna o registro.

O status 409 evita escolher arbitrariamente um dispositivo quando a data possui múltiplos rastreadores.

## 13.11 Endpoint de rota

### `GET /api/v1/routes/{event_date}`

Parâmetro opcional `device_serial`.

Fluxo:

1. normaliza o serial;
2. lista dispositivos com pontos naquela data;
3. retorna 404 se não houver nenhum;
4. se o serial não foi informado e houver mais de um dispositivo, retorna 409;
5. se o serial informado não existir naquela data, retorna 404;
6. lê os pontos do dispositivo escolhido;
7. exige pelo menos dois pontos, ou retorna 422;
8. monta coordenadas em ordem GeoJSON `[longitude, latitude]`;
9. calcula início, fim, quantidade de pontos e velocidade máxima;
10. retorna três features: LineString, ponto inicial e ponto final.

### 13.11.1 Estrutura resumida da resposta

```json
{
  "type": "FeatureCollection",
  "features": [
    {"type": "Feature", "geometry": {"type": "LineString"}},
    {"type": "Feature", "geometry": {"type": "Point"}, "properties": {"role": "start"}},
    {"type": "Feature", "geometry": {"type": "Point"}, "properties": {"role": "end"}}
  ]
}
```

## 13.12 Execução da API

```powershell
uv run uvicorn src.api.main:app --reload
```

Endereços locais:

- API: `http://127.0.0.1:8000`;
- Swagger: `http://127.0.0.1:8000/docs`;
- OpenAPI JSON: `http://127.0.0.1:8000/openapi.json`.

A API depende da Gold já ter sido gerada. `/health` pode retornar 200 mesmo sem tabelas; use `/ready` para confirmar disponibilidade real.

# 14. Fluxo Data Lake em Parquet

O repositório mantém uma implementação paralela em `data/lake`. A lógica de negócio é quase a mesma, mas a persistência não usa log Delta.

## 14.1 `lake_01_bronze.py`

- lê o mesmo CSV;
- cria `data/lake/01_bronze`;
- grava `logs_rastreador_2026-07-01.parquet` por `DataFrame.to_parquet`;
- usa `index=False` para não persistir o índice pandas;
- informa quantidade de linhas.

## 14.2 `lake_02_silver.py`

Diferenças principais:

- lê Parquet por `read_parquet` no DuckDB;
- a opção `filename = TRUE` fornece `source_file` real;
- normaliza caminhos para SQL com `as_posix()` e escape de aspas;
- usa `COPY (...) TO ... (FORMAT PARQUET, COMPRESSION ZSTD, PARTITION_BY, OVERWRITE TRUE)`;
- a lógica de telemetria, identidade e rejeição é equivalente à Silver Delta.

## 14.3 `lake_03_gold.py`

Além da lógica analítica equivalente, precisa de funções específicas do formato arquivo:

- `normalize_sql_path`: caminho compatível com SQL;
- `parquet_glob`: cria `/**/*.parquet` para ler partições;
- `validate_parquet_dataset`: exige diretório e pelo menos um arquivo;
- `clear_output`: remove saídas antigas, pois não existe `mode="overwrite"` transacional equivalente ao Delta;
- `read_parquet(..., hive_partitioning=TRUE, union_by_name=TRUE)`: lê partições e tolera evolução de colunas por nome.

## 14.4 `lake_pipeline.py`

Executa Bronze, Silver e Gold Parquet na mesma ordem do Lakehouse.

## 14.5 Diferença prática

| Aspecto | Parquet puro | Delta Lake |
| --- | --- | --- |
| Estrutura | Arquivos e diretórios | Arquivos Parquet + _delta_log |
| Estado da tabela | Inferido pelos arquivos presentes | Definido por snapshot no log |
| Sobrescrita | COPY/remoção de diretórios | Transação Delta overwrite |
| Validação | Procura arquivos .parquet | Valida Delta Table |
| Versionamento | Não disponível nativamente | Versões registradas no log |
| API atual | Não consome | Consome somente a Gold Delta |

# 15. Execução passo a passo

## 15.1 Passo 1 — preparar o CSV

Confirme que o arquivo existe:

```powershell
Test-Path .\data
aw\logs_rastreador_2026-07-01.csv
```

O resultado deve ser `True`.

## 15.2 Passo 2 — instalar dependências

```powershell
uv sync
```

## 15.3 Passo 3 — executar somente a Bronze

```powershell
uv run src/lakehouse_01_bronze.py
```

Valide a presença de:

```text
data/lakehouse/01_bronze/logs_rastreador_2026-07-01/_delta_log
```

## 15.4 Passo 4 — executar a Silver

```powershell
uv run src/lakehouse_02_silver.py
```

Valide:

```text
data/lakehouse/02_silver/telemetry_events/_delta_log
data/lakehouse/02_silver/device_identity_events/_delta_log
data/lakehouse/02_silver/rejected_logs/_delta_log
```

## 15.5 Passo 5 — executar a Gold

```powershell
uv run src/lakehouse_03_gold.py
```

Valide as cinco tabelas em `03_gold`.

## 15.6 Alternativa — pipeline completo

```powershell
uv run src/lakehouse_pipeline.py
```

## 15.7 Passo 6 — iniciar a API

```powershell
uv run uvicorn src.api.main:app --reload
```

## 15.8 Passo 7 — verificar operacionalmente

Abra em sequência:

1. `/health`;
2. `/ready`;
3. `/api/v1/data-quality`;
4. `/api/v1/daily-summary`;
5. `/api/v1/routes/2026-07-01`.

Quando houver mais de um dispositivo na data, acrescente `?device_serial=SERIAL`.

# 16. Validação dos resultados

## 16.1 Inspecionar versão e schema de uma tabela

```powershell
uv run python -c "from deltalake import DeltaTable; t=DeltaTable('data/lakehouse/03_gold/device_daily_summary'); print(t.version()); print(t.schema())"
```

## 16.2 Contar linhas com DuckDB

```powershell
uv run python -c "import duckdb; print(duckdb.sql("select count(*) from delta_scan('data/lakehouse/02_silver/telemetry_events')").fetchone())"
```

Se a extensão Delta do DuckDB não estiver disponível no ambiente, use `DeltaTable(...).to_pyarrow_dataset()` como o próprio projeto faz.

## 16.3 Reconciliação básica

Para a qualidade de uma data:

```text
accepted_event_count = telemetry_event_count + identity_event_count
total_event_count = accepted_event_count + rejected_event_count
```

## 16.4 Verificações de integridade recomendadas

- todo `telemetry_events.message_type` segue Tn e é diferente de T1;
- todo `device_identity_events.message_type` é T1;
- nenhuma linha aceita tem `event_timestamp` ou `device_serial` nulo;
- toda rejeição possui `rejection_reason`;
- `dim_device.device_serial` é único;
- `device_last_position.device_serial` é único;
- `device_route_points.point_sequence` é crescente por dispositivo/data;
- `device_daily_summary` é único por dispositivo/data;
- percentuais ficam entre 0 e 100;
- rotas não contêm 0,0.

## 16.5 Diferença entre validação estrutural e semântica

O pipeline atual garante principalmente estrutura mínima. Ele não confirma unidade, plausibilidade física ou coerência temporal de todos os valores. Uma velocidade extrema, por exemplo, pode ser aceita se for numérica.

# 17. Regras de negócio consolidadas

## 17.1 Árvore de decisão de uma linha Bronze

```text
MESS_TYPE está vazio?
  sim → rejeitar: MISSING_MESSAGE_TYPE
  não ↓

MESS_TYPE corresponde a T + dígitos?
  não → rejeitar: INVALID_MESSAGE_TYPE
  sim ↓

Existe TM_STAMP ou DATA_SERVIDOR válido?
  não → rejeitar: MISSING_OR_INVALID_TIMESTAMP
  sim ↓

Existe serial?
  não → rejeitar: MISSING_DEVICE_SERIAL
  sim ↓

É T1?
  sim → device_identity_events
  não → telemetry_events
```

## 17.2 Resposta direta: mensagem sem timestamp

- Se `TM_STAMP` estiver ausente ou inválido, mas `DATA_SERVIDOR` for válido, a mensagem é aceita e usa o timestamp do servidor.
- Se `DATA_SERVIDOR` estiver ausente ou inválido, mas `TM_STAMP` for válido, a mensagem é aceita e usa o timestamp do dispositivo.
- Se ambos estiverem ausentes ou inválidos, a mensagem vai para `rejected_logs` com `MISSING_OR_INVALID_TIMESTAMP` e `rejection_date = unknown`.

## 17.3 Resposta direta: coordenada ruim

Coordenada ruim não rejeita o evento. Ela altera `has_valid_coordinates` e `position_quality`. A Gold decide quais produtos podem usar o ponto.

## 17.4 Resposta direta: identificador T1 inválido

A mensagem T1 continua na tabela de identidade, mas a flag de formato correspondente fica falsa.

# 18. Tratamento de erros e troubleshooting

## 18.1 `The file ... does not exist`

Causa: o CSV não está no nome e caminho fixos.  
Ação: colocar o arquivo no local correto ou parametrizar a Bronze.

## 18.2 `The Bronze Delta Table does not exist`

Causa: Bronze não foi executada, falhou ou foi apagada.  
Ação: executar `lakehouse_01_bronze.py` e confirmar o diretório.

## 18.3 `The Bronze path is not a valid Delta Table`

Causa: o diretório existe, mas não contém `_delta_log`.  
Ação: apagar a saída incompleta e recriar a Bronze.

## 18.4 Erro de coluna não encontrada no DuckDB

Causa: cabeçalho do CSV diferente do contrato.  
Ação: comparar os nomes, inclusive `S/N ou IMEI` e `LONT`.

## 18.5 Erro de leitura do CSV

Possíveis causas:

- delimitador diferente de vírgula;
- encoding diferente;
- aspas ou linhas quebradas;
- arquivo grande demais para memória.

Ação: configurar explicitamente `sep`, `encoding`, `dtype` e estratégia de linhas inválidas.

## 18.6 `/health` funciona, mas `/ready` retorna 503

Causa: servidor está vivo, porém uma ou mais tabelas Gold não existem ou não são Delta válidas.  
Ação: executar o pipeline e ler o campo `tables` da resposta de readiness.

## 18.7 Endpoint retorna 409

Causa: mais de um dispositivo existe na data e o serial foi omitido.  
Ação: usar `?device_serial=...`.

## 18.8 Rota retorna 422

Causa: depois dos filtros restaram menos de dois pontos. Uma LineString exige pelo menos dois pontos no contrato da API.

## 18.9 Dados antigos aparecem após mudar o CSV

Verifique se executou as três camadas. Executar apenas a Bronze não atualiza a Silver ou a Gold.

# 19. Limitações e inconsistências atuais

## 19.1 Versão do pacote e da API divergente

- `pyproject.toml`: versão 0.1.0;
- FastAPI: versão 0.5.0.

Pode ser intencional, mas sem documentação causa confusão sobre release.

## 19.2 Arquivo de entrada rigidamente nomeado

A data está embutida em caminho, tabela Bronze e `source_file`. O pipeline não processa automaticamente outro arquivo.

## 19.3 README afirma que o CSV original está incluído

O `.gitignore` exclui os arquivos Raw, salvo exceções específicas. O fluxo de onboarding deve explicar como obter ou gerar a entrada.


## 19.4 Bronze sem metadados técnicos

Não há `ingestion_timestamp`, checksum, nome real do arquivo, tamanho, batch_id ou versão de schema.

## 19.5 Full refresh em todas as camadas

Não existe append incremental, merge, watermark ou controle de arquivos já processados.

## 19.6 Materialização em pandas

Silver e Gold executam SQL no DuckDB, mas chamam `.df()` antes de escrever Delta. Em volume grande, o resultado inteiro ocupa memória do processo.

## 19.7 Regras de qualidade incompletas

Valores numéricos inválidos viram `NULL`; velocidades impossíveis e regressões temporais não são rejeitadas. Isso pode ser adequado para preservar dados, mas exige métricas adicionais.

## 19.8 Timezone não explicitado

Os timestamps são convertidos para `TIMESTAMP` sem normalização de fuso. Comparações entre dispositivo e servidor podem ficar ambíguas.

## 19.9 `source_file` sintético no Lakehouse

A Silver reconstrói o nome a partir do diretório. Se a Bronze agregar múltiplos arquivos, a linhagem por linha será perdida.

## 19.10 Publicação não atômica entre tabelas

Cada tabela é sobrescrita separadamente. Uma falha no meio pode deixar camadas com versões incompatíveis.

## 19.11 API carrega tabelas completas

`read_gold_table` materializa todo o dataset antes de filtrar em Python. Não há paginação ou pushdown.

## 19.12 Produtos Gold sem endpoint

`dim_device` e `device_last_position` são verificados em `/ready`, mas ainda não possuem endpoints públicos dedicados.

## 19.13 Ausência de testes automatizados

O repositório não contém suíte de testes. O bloco `__main__` do reader é apenas verificação manual.

## 19.14 CORS não configurado

Um dashboard executado em outra origem pode precisar de `CORSMiddleware`.

## 19.15 Regras 0,0 não são uniformes

Rota e última posição excluem 0,0; o resumo diário conta 0,0 como coordenada válida porque usa a flag Silver. A definição de posição válida para KPI deve ser unificada.

# 20. Roadmap técnico recomendado

## 20.1 Prioridade imediata

1. avaliar aproveitamento de dados incompletos (ex.: sem coordenada, sem timestamp, etc);
2. subir aplicação para o servidor;
3. parametrizar arquivo/data de entrada;
4. adicionar `source_file`, `ingested_at` e `batch_id` na Bronze;
5. criar testes para regras de rejeição e qualidade;
6. documentar unidades do protocolo.

## 20.2 Próxima etapa

1. processamento de múltiplos arquivos;
2. escrita incremental com append/merge;
3. tabela de controle de ingestão;
4. validação de schema antes de processar;
5. publicação atômica por batch;
6. consultas da API com filtros pushdown;
7. endpoints de dispositivos e última posição;
8. paginação e limites de resposta;
9. CORS e configuração por ambiente.

## 20.3 Evolução analítica

- distância diária por Haversine;
- detecção de viagens e paradas;
- velocidade impossível e saltos de GPS;
- última atividade por dispositivo;
- indicadores de completude por campo;


# 21. Glossário

| Termo | Definição no contexto do projeto |
| --- | --- |
| ACID | Propriedades transacionais que tornam uma alteração de tabela consistente e recuperável. |
| Arrow/PyArrow | Formato e biblioteca colunares usados para interoperabilidade em memória. |
| Batch | Conjunto de registros processado em uma execução. |
| CTE | Common Table Expression; consulta temporária definida por WITH. |
| Delta log | Histórico transacional armazenado em _delta_log. |
| Delta Table | Tabela composta por Parquet e log Delta. |
| DuckDB | Motor SQL analítico embutido no processo Python. |
| Full refresh | Reconstrução completa da saída. |
| Grain | O que uma linha representa. |
| HDOP | Indicador de geometria dos satélites e precisão horizontal; menor costuma ser melhor. |
| ICCID | Identificador do cartão SIM. |
| IMEI | Identificador do equipamento móvel. |
| IMSI | Identificador do assinante na rede móvel. |
| Idempotência | Reexecutar com a mesma entrada produz o mesmo estado final. |
| Partição | Organização física por valor, como event_date. |
| Pushdown | Aplicar filtro na fonte antes de materializar todos os dados. |
| Schema enforcement | Validação da estrutura e tipos da tabela. |
| Snapshot | Estado consistente da Delta Table em uma versão. |
| Telemetry | Medições e estados enviados pelo rastreador. |
| Time travel | Leitura de versões anteriores de uma tabela Delta. |

# 22. Apêndices

## Apêndice A — Matriz de linhagem

| Origem | Destino | Transformação principal |
| --- | --- | --- |
| CSV Raw | Bronze | Cópia integral e conversão para Delta. |
| Bronze | bronze_normalized | Rename, trim, vazio→NULL, timestamps. |
| bronze_normalized | telemetry_events | Tn exceto T1, tipagem numérica, qualidade GPS. |
| bronze_normalized | device_identity_events | T1, mapeamento ICCID/IMSI/IMEI, flags de formato. |
| bronze_normalized | rejected_logs | Quatro regras estruturais e motivo prioritário. |
| telemetry_events | telemetry_gold_base | Deduplicação lógica. |
| device_identity_events | identity_gold_base | Deduplicação lógica. |
| bases Gold | dim_device | Agregação por dispositivo e seleção dos valores atuais. |
| telemetry_gold_base | device_last_position | Último ponto válido não 0,0. |
| telemetry_gold_base | device_route_points | Pontos válidos ordenados por dia/dispositivo. |
| telemetry_gold_base | device_daily_summary | Agregações diárias. |
| Silver completa | data_quality_summary | Contagens de aceitos e rejeitados. |
| Gold | API | Leitura, filtro, validação e serialização. |

## Apêndice B — Comandos de referência

```powershell
# instalar dependências
uv sync

# pipeline Delta completo
uv run src/lakehouse_pipeline.py

# camadas Delta separadas
uv run src/lakehouse_01_bronze.py
uv run src/lakehouse_02_silver.py
uv run src/lakehouse_03_gold.py

# pipeline Parquet completo
uv run src/lake_pipeline.py

# API
uv run uvicorn src.api.main:app --reload

# teste manual do reader
uv run src/api/lakehouse_reader.py
```

## Apêndice C — Referências de código

- [README](https://github.com/Zuimbra/local-lakehouse-experiment/blob/main/README.md)
- [pyproject.toml](https://github.com/Zuimbra/local-lakehouse-experiment/blob/main/pyproject.toml)
- [Bronze Lakehouse](https://github.com/Zuimbra/local-lakehouse-experiment/blob/main/src/lakehouse_01_bronze.py)
- [Silver Lakehouse](https://github.com/Zuimbra/local-lakehouse-experiment/blob/main/src/lakehouse_02_silver.py)
- [Gold Lakehouse](https://github.com/Zuimbra/local-lakehouse-experiment/blob/main/src/lakehouse_03_gold.py)
- [Pipeline Lakehouse](https://github.com/Zuimbra/local-lakehouse-experiment/blob/main/src/lakehouse_pipeline.py)
- [Reader da API](https://github.com/Zuimbra/local-lakehouse-experiment/blob/main/src/api/lakehouse_reader.py)
- [FastAPI](https://github.com/Zuimbra/local-lakehouse-experiment/blob/main/src/api/main.py)
- [.gitignore](https://github.com/Zuimbra/local-lakehouse-experiment/blob/main/.gitignore)

## Apêndice D — Checklist de conclusão

Uma implantação local está concluída quando:

- [ ] `uv sync` termina sem erro;
- [ ] o CSV está no caminho esperado;
- [ ] Bronze possui `_delta_log`;
- [ ] as três tabelas Silver existem;
- [ ] as cinco tabelas Gold existem;
- [ ] `/health` retorna 200;
- [ ] `/ready` retorna `ready`;
- [ ] os endpoints analíticos retornam dados;
- [ ] contagens de qualidade reconciliam;
- [ ] as rotas possuem pelo menos dois pontos válidos;
- [ ] as limitações de unidade, timezone e full refresh são conhecidas pelos consumidores.
