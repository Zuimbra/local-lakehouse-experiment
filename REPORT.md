# Reconstrução Técnica Completa do `local-lakehouse-experiment`

## Evolução cronológica orientada pelo código

## 1. Como esta reconstrução deve ser lida

Este documento reconstrói o projeto na mesma ordem em que suas capacidades apareceram no Git.

A unidade principal é uma **mudança técnica observável no código**, e não uma Sprint ou simplesmente um commit.

A estrutura de cada passo é:

# O que?

Qual comportamento foi introduzido ou alterado.

# Para que?

Qual limitação concreta do estado anterior tornou a mudança necessária.

# Como?

A implementação é percorrida na ordem real:

```text
trecho de código
↓
o que esse trecho faz
↓
próximo trecho
↓
como ele recebe o resultado anterior
↓
próximo trecho
↓
efeito produzido
```

Quando uma mudança substitui outra implementação, o código anterior aparece antes do novo.

Quando há uma operação Delta, a operação responsável é mostrada.

Quando há um contrato entre camadas, sua definição, produção e consumo são acompanhados.

Quando um bug aparece, a sequência que o produz e a alteração que o corrige fazem parte do passo.

O histórico analisado contém 22 commits entre o commit inicial `04f176b`, de 14 de julho de 2026, e `ffc6b41`, de 11 de agosto de 2026.

---

# Parte I — Antes de existir pipeline

# Passo 1 — O projeto nasce antes do código executável

**Commit relacionado:** `04f176b` — `Inicial Commit`

## O que?

O primeiro commit cria o repositório com documentação e desenho arquitetural, mas ainda não existe código de ingestão.

O estado inicial é:

```text
README
+
lakehouse.excalidraw
↓
nenhum loader
nenhuma Bronze
nenhuma Silver
nenhuma Gold
```

O histórico confirma que `04f176b` é o primeiro commit da sequência analisada.

## Para que?

Esse ponto é importante porque impede interpretar o projeto atual como algo que surgiu pronto.

Primeiro existe a intenção de experimentar uma arquitetura de dados local. A implementação começa somente quando um dataset concreto é colocado no repositório.

## Como?

Não há função para percorrer neste ponto.

Essa ausência de código é justamente o estado técnico relevante:

```text
arquitetura pensada
↓
ainda não existe dado para processar
```

A próxima alteração introduz a matéria-prima sobre a qual todo o restante do projeto será construído.

---

# Passo 2 — O primeiro CSV transforma a arquitetura em um problema concreto

**Commit relacionado:** `caa0738` — `Add raw data`

## O que?

É adicionado:

```text
data/raw/logs_rastreador_2026-07-01.csv
```

O dataset passa a existir antes das camadas que irão processá-lo.

## Para que?

A partir desse momento o desenvolvimento deixa de trabalhar apenas com a ideia abstrata de:

```text
Raw → Bronze → Silver → Gold
```

e passa a lidar com:

```text
um CSV real
↓
colunas reais
↓
tipos de mensagem reais
↓
problemas reais de interpretação
```

Isso será particularmente importante na Silver, porque o mesmo layout físico contém registros com significados diferentes.

---

# Passo 3 — Data Lake e Lakehouse ganham espaços físicos separados

**Commit relacionado:** `a9a185f` — `Add inicial directory structure`

## O que?

A estrutura de diretórios introduz áreas separadas para:

```text
data/lake/
data/lakehouse/
notebook/
```

O Data Lake e o Lakehouse não aparecem, portanto, como a mesma implementação. O histórico mostra a área do Data Lake sendo utilizada primeiro.

## Para que?

Essa separação permitirá construir inicialmente:

```text
CSV
↓
Parquet
```

e somente depois reproduzir a arquitetura utilizando:

```text
Delta Table
```

A distinção será visível no próprio código de escrita.

---

# Parte II — O Data Lake é construído primeiro

# Passo 4 — Primeira Bronze: um CSV fixo é convertido para Parquet

**Commit relacionado:** `110f9cc`
**Arquivo:** `src/lake_01_bronze.py`

# O que?

A primeira Bronze executável aparece como uma função sem parâmetros:

```python
def load_bronze_data():
```

Ela ainda não sabe descobrir arquivos. A própria função define previamente qual arquivo será processado.

# Para que?

O objetivo inicial é mínimo: transformar o CSV bruto em uma representação analítica dentro do Data Lake.

Nesse estágio existe apenas um arquivo conhecido, então ainda não há necessidade de:

```text
inbox
hash
batch
controle
MERGE
```

# Como?

A origem é montada diretamente dentro de `load_bronze_data()`.

O nome:

```text
logs_rastreador_2026-07-01.csv
```

faz parte do próprio path.

Isso significa que a função não recebe a origem da execução. A origem já está decidida antes de ela começar.

A leitura acontece então com:

```python
df = pd.read_csv(raw_path)
```

Nesse ponto:

```text
CSV
↓
Pandas DataFrame
```

Nenhum metadado técnico é adicionado. Nenhuma classificação de mensagem ocorre. A Bronze simplesmente materializa o dado recebido.

A persistência é feita com:

```python
df.to_parquet(output_path, index=False)
```

A primeira Bronze, portanto, termina em:

```text
data/raw/logs_rastreador_2026-07-01.csv
↓
pd.read_csv()
↓
DataFrame
↓
to_parquet()
↓
data/lake/01_bronze/
```

O comportamento fundamental dessa primeira versão é:

```text
um arquivo conhecido
↓
uma execução
↓
um Parquet produzido novamente
```

Ainda não existe processamento incremental.

---

# Passo 5 — A Silver começa a descobrir o significado dos registros

**Commits relacionados:** `110f9cc` e correção `2d1d9c4`
**Arquivo:** `src/lake_02_silver.py`

# O que?

A Silver deixa de tratar todas as linhas como o mesmo tipo de dado.

Ela cria três destinos:

```text
telemetry_events
device_identity_events
rejected_logs
```

A separação é construída com SQL executado no DuckDB.

# Para que?

A estrutura do CSV cria um problema que não existe na Bronze.

As colunas:

```text
BAT_VOLT
LAT
LONT
```

não podem ser convertidas imediatamente para tipos numéricos.

Para telemetria, elas representam grandezas como bateria e coordenadas.

Para `T1`, posições do mesmo protocolo são utilizadas como identificadores.

Logo:

```text
mesma posição física
+
tipo de mensagem diferente
↓
semântica diferente
```

Essa necessidade está refletida diretamente na forma como `bronze_normalized` mantém inicialmente determinados campos como texto.

# Como?

A Silver começa lendo o arquivo produzido pela Bronze.

Na correção `2d1d9c4`, o path passa a apontar corretamente para:

```text
data/lake/01_bronze/
```

em vez de uma localização incoerente com o arquivo realmente produzido pela etapa anterior. O diff desse commit mostra explicitamente essa correção de integração.

Depois da leitura, é criada a view:

```sql
CREATE OR REPLACE TEMP VIEW bronze_normalized
```

Essa view é a primeira fronteira de tratamento.

Nela são feitos:

```text
TRIM
"" → NULL
TRY_CAST de timestamps
preservação temporária de campos ambíguos como texto
```

A Silver não começa criando `telemetry_events`. Primeiro ela produz uma representação normalizada a partir da qual as três classificações seguintes serão derivadas.

A seleção de telemetria contém a exclusão explícita:

```sql
AND message_type <> 'T1'
```

Essa linha é pequena, mas altera completamente o fluxo: registros `T1` não entram no caminho que converte latitude, longitude, velocidade e bateria para tipos telemétricos.

A cadeia torna-se:

```text
bronze_normalized
↓
message_type segue padrão Tn?
↓
é diferente de T1?
├── sim → candidato a telemetria
└── não → outro fluxo
```

Dentro da telemetria, os campos finalmente podem receber tipos numéricos porque o significado da linha já foi determinado.

Em seguida a Silver avalia a posição.

A classificação é feita dentro do SQL, não posteriormente em Python:

```text
latitude/longitude ausentes
→ MISSING_COORDINATES

latitude/longitude fora dos limites
→ INVALID_COORDINATES

HDOP elevado
→ LOW_GPS_PRECISION

caso contrário
→ VALID
```

A consequência é que `position_quality` já nasce durante a transformação que produz `telemetry_events`.

O caminho de `T1` utiliza os campos de maneira diferente. Um dos mapeamentos aparece como:

```sql
battery_voltage_raw AS iccid
```

Outros slots são reinterpretados como IMSI e IMEI.

Portanto, a execução real é:

```text
linha T1
↓
não entra na telemetria
↓
campos brutos permanecem texto
↓
slots são reinterpretados
↓
ICCID / IMSI / IMEI
↓
device_identity_events
```

Os identificadores são então submetidos a regras de formato, como comprimento e composição numérica.

O terceiro caminho trata registros que não cumprem os contratos mínimos.

A lógica associa motivos como:

```text
MISSING_MESSAGE_TYPE
INVALID_MESSAGE_TYPE
MISSING_OR_INVALID_TIMESTAMP
MISSING_DEVICE_SERIAL
```

A regra de timestamp é particularmente importante: ausência de um timestamp utilizável não faz a linha desaparecer.

Ela é preservada em `rejected_logs`.

Quando nem `TM_STAMP` nem `DATA_SERVIDOR` permitem determinar uma data, a expressão de particionamento produz:

```text
rejection_date = "unknown"
```

Isso cria, já na primeira Silver, uma decisão que continuará relevante até a incrementalidade: dado inválido é preservado e classificado, não silenciosamente descartado.

Por fim, os três resultados são gravados como Parquet.

A Silver inicial usa sobrescrita e particiona os datasets temporais por suas respectivas datas.

O fluxo completo agora é:

```text
Bronze Parquet
↓
bronze_normalized
↓
interpretar message_type
├── T1
│   ↓
│   device_identity_events
│
├── T2, T3, T4...
│   ↓
│   telemetry_events
│
└── estruturalmente inválido para Silver
    ↓
    rejected_logs
```

---

# Passo 6 — A Gold começa deduplicando antes de produzir informação de consumo

**Commit relacionado:** `fba59ea` — `Lake Gold Layer`
**Arquivo:** `src/lake_03_gold.py`

# O que?

A primeira Gold cria:

```text
dim_device
device_last_position
device_daily_summary
data_quality_summary
```

Mas os produtos não são construídos diretamente sobre os arquivos Silver sem tratamento adicional.

# Para que?

A Silver preserva os eventos classificados.

A Gold precisa impedir que retransmissões equivalentes inflem:

```text
contagens
resumos
estado do dispositivo
```

Por isso a primeira operação relevante da Gold é construir uma base deduplicada.

# Como?

Antes de ler a Silver, a implementação antiga já revela sua estratégia de atualização:

```python
clear_output(dim_device_path)
```

O mesmo padrão é aplicado aos demais produtos.

Ou seja:

```text
Gold anterior
↓
remover saída
↓
recalcular
↓
gravar novamente
```

É um **full refresh** explícito.

Em seguida, DuckDB abre os Parquets Silver usando `hive_partitioning` e `union_by_name`.

A Gold recebe então os três datasets:

```text
silver_telemetry
silver_identity
silver_rejected
```

Sobre `silver_telemetry`, é criada:

```sql
CREATE OR REPLACE TEMP VIEW telemetry_gold_base
```

A deduplicação usa `ROW_NUMBER()` sobre uma combinação que inclui dispositivo, timestamp, tipo, contador e propriedades da posição.

A consequência é:

```text
eventos Silver
↓
agrupar retransmissões equivalentes
↓
ordenar versões
↓
ROW_NUMBER() = 1
↓
telemetry_gold_base
```

Somente depois dessa base os produtos são calculados.

Para `dim_device`, por exemplo, a query agrupa identidade por:

```sql
GROUP BY device_serial
```

e utiliza agregações temporais e `ARG_MAX` para recuperar os identificadores mais recentes.

A transformação lógica é:

```text
múltiplos T1
+
múltiplos eventos telemétricos
↓
device_serial
↓
primeira ocorrência
última ocorrência
identidade mais recente
contagens
↓
uma linha em dim_device
```

`device_last_position` aplica outra semântica:

```text
somente posições válidas
↓
ordenar eventos do dispositivo do mais novo para o mais antigo
↓
escolher o primeiro
```

`device_daily_summary` não representa estado, mas agregação:

```text
event_date + device_serial
↓
COUNT
AVG
MIN
MAX
ARG_MIN
ARG_MAX
↓
uma linha por dispositivo/dia
```

A versão atual da mesma transformação ainda mostra métricas como `message_count`, posições válidas e inválidas, velocidade, HDOP, bateria e odômetro, permitindo identificar a origem desses produtos na primeira Gold.

Por fim, `data_quality_summary` combina:

```text
telemetria aceita
+
identidade aceita
+
rejeitados
↓
métricas por data
```

A primeira Gold já separa, portanto:

```text
evento
estado
agregação
qualidade
```

embora sua escrita ainda seja integral.

---

# Passo 7 — Surge o marco do pipeline do Data Lake, mas o histórico exige cautela

**Commit relacionado:** `0d900ac` — `Lake Pipeline`

# O que?

O histórico registra um commit chamado `Lake Pipeline` imediatamente depois da criação da Gold.

# Para que?

Cronologicamente, esse é o ponto em que Bronze, Silver e Gold passam a ser tratadas como uma cadeia de execução, e não apenas scripts independentes.

# Como?

Aqui existe uma limitação de evidência importante.

O snapshot acessível desse commit é apresentado pelo GitHub como alteração essencialmente de whitespace, e o conteúdo executável do `lake_pipeline.py` não pôde ser reconstruído de maneira segura diretamente desse snapshot.

Portanto, não é correto inventar um trecho como:

```text
load_bronze_data()
load_silver_data()
load_gold_data()
```

e atribuí-lo automaticamente a `0d900ac`.

O primeiro orquestrador sequencial que pode ser verificado diretamente em uma versão histórica posterior é o `lakehouse_pipeline.py`, que contém as três chamadas em sequência.

Essa diferença entre:

```text
commit cujo nome indica o nascimento do pipeline
```

e:

```text
código executável diretamente recuperável
```

é mantida explicitamente nesta reconstrução.

---

# Parte III — O Data Lake é reproduzido como Lakehouse

# Passo 8 — A Bronze troca Parquet puro por Delta Table

**Commit relacionado:** `c9d42c7` — `Lakehouse`
**Arquivo:** `src/lakehouse_01_bronze.py`

# O que?

A arquitetura já experimentada no Data Lake é reproduzida em uma nova implementação baseada em Delta Lake.

A alteração fica clara comparando as duas Bronzes.

## Antes — Data Lake

```python
df.to_parquet(output_path, index=False)
```

A saída é um arquivo Parquet.

## Depois — Lakehouse

A função importa:

```python
from deltalake import write_deltalake
```

e passa a persistir o DataFrame como uma Delta Table.

# Para que?

O projeto não joga fora o desenho Bronze/Silver/Gold anterior.

Ele reaproveita a arquitetura e altera principalmente a tecnologia de persistência:

```text
Data Lake
CSV → Parquet

Lakehouse
CSV → Delta
```

O próprio estado do repositório nesse commit descreve o Data Lake em Parquet como etapa anterior ao experimento Lakehouse.

# Como?

A leitura continua essencialmente igual:

```python
df = pd.read_csv(raw_path)
```

O arquivo também continua fixo.

Logo, o surgimento do Delta **não** resolve ainda a ingestão de múltiplos arquivos.

A mudança está na persistência:

```text
write_deltalake(...)
mode = overwrite
```

A primeira versão Lakehouse é, portanto:

```text
CSV fixo
↓
Pandas
↓
Delta Table
↓
overwrite
```

Isso é importante: **Delta Lake existe antes da incrementalidade**.

Nesse ponto ainda não existem:

```text
row_id
batch_id
MERGE
controle
inbox
affected_event_dates
NOOP
```

---

# Passo 9 — O primeiro pipeline Lakehouse diretamente verificável é puramente sequencial

**Commit relacionado:** estado recuperável em `49130cd`
**Arquivo:** `src/lakehouse_pipeline.py`

# O que?

O pipeline contém três chamadas:

```python
load_bronze_data()
load_silver_data()
load_gold_data()
```

# Para que?

A sequência garante a dependência:

```text
Bronze precisa existir
↓
Silver pode ser criada
↓
Gold pode ser criada
```

# Como?

Não existe variável capturando o resultado da Bronze.

Não há algo equivalente a:

```text
bronze_result = ...
```

A função simplesmente termina e a próxima começa.

Portanto:

```text
Bronze
↓
concluiu?
↓
Silver

Silver
↓
concluiu?
↓
Gold
```

O pipeline conhece a **ordem**, mas não conhece o **impacto**.

Essa será a principal transformação da orquestração no final do histórico.

---

# Passo 10 — A Gold ganha uma representação ordenada de rota

**Commit relacionado:** `58acdfd`
**Arquivo:** `src/lakehouse_03_gold.py`

# O que?

É criado:

```text
device_route_points
```

# Para que?

`device_last_position` representa estado atual.

Uma rota precisa preservar uma sequência.

Logo, não basta saber que um ponto existe; é necessário saber:

```text
qual veio antes?
qual veio depois?
```

# Como?

A query começa restringindo a fonte a coordenadas válidas e descartando `(0, 0)`.

Depois é criada uma CTE chamada:

```sql
ordered_points
```

Dentro dela, `ROW_NUMBER()` particiona os registros por:

```text
device_serial
+
event_date
```

e os ordena por timestamp e critérios auxiliares.

A coluna resultante é:

```sql
AS point_sequence
```

Isso materializa:

```text
dispositivo + dia
↓
ordenar pontos no tempo
↓
1, 2, 3, 4...
↓
device_route_points
```

A primeira implementação ainda grava a tabela inteira com:

```text
mode = overwrite
```

e particionamento por `event_date`.

Esse detalhe será determinante quando late-arriving data surgir: inserir um ponto antigo pode alterar o `point_sequence` dos pontos posteriores.

---

# Passo 11 — A API coloca um contrato HTTP na frente da Gold

**Commit relacionado:** `7bc4f03`
**Arquivos:** `src/api/main.py`, `src/api/lakehouse_reader.py`

# O que?

É criada uma aplicação FastAPI versão `0.5.0` que consulta produtos Gold.

# Para que?

Antes:

```text
consumidor
↓
precisa conhecer data/lakehouse/03_gold
↓
precisa abrir Delta
```

Depois:

```text
consumidor
↓
HTTP
↓
FastAPI
↓
reader
↓
Gold
```

A estrutura física deixa de ser o contrato externo.

# Como?

O reader declara quais tabelas podem ser acessadas:

```python
GOLD_TABLES = (
    "data_quality_summary",
    ...
)
```

O conjunto inclui qualidade, resumo diário, última posição, rotas e dimensão de dispositivos.

Quando uma consulta é feita, a tabela é aberta como `DeltaTable`.

Depois aparece a passagem:

```text
DeltaTable
↓
to_pyarrow_dataset()
↓
to_table()
↓
to_pylist()
```

A API recebe estruturas Python em vez de exigir que o endpoint saiba manipular arquivos Delta diretamente.

A aplicação é criada por:

```python
app = FastAPI(...)
```

com versão `0.5.0`.

O endpoint `/health` apenas confirma que o processo responde.

`/ready`, por outro lado, percorre `GOLD_TABLES`.

Se uma tabela não puder ser descrita:

```text
all_tables_ready = False
↓
HTTP 503
```

Isso diferencia:

```text
API está viva
```

de:

```text
API consegue servir o Lakehouse
```

Os endpoints analíticos então delegam a leitura ao reader, por exemplo passando `date_from`, `date_to` e, quando aplicável, `device_serial`.

Nesse momento a arquitetura já é:

```text
Raw
↓
Lakehouse Delta
↓
Silver
↓
Gold
↓
FastAPI
```

Mas a ingestão continua limitada.

É contra essa versão que a modernização seguinte acontece.

---

# Parte IV — A Bronze é reconstruída como processo de ingestão

# Passo 12 — A `inbox` aparece antes da ingestão múltipla real

**Commit relacionado:** `8b661e7`
**Arquivo:** `src/lakehouse_01_bronze.py`

# O que?

A função passa a criar:

```text
inbox
archive
quarantine
```

e surge:

```python
def discover_input_files(
```

# Para que?

Antes, o arquivo fazia parte do path fixo da Bronze.

Isso impede a operação:

```text
arquivo A chega hoje
arquivo B chega amanhã
arquivo C chega depois
```

sem modificar ou parametrizar a aplicação.

# Como?

A primeira alteração cria os diretórios:

```text
data/raw/inbox
data/raw/archive
data/raw/quarantine
```

Mas o próprio comentário histórico deixa claro que `archive` e `quarantine` ainda estão apenas preparados.

Depois a descoberta percorre diretamente a `inbox`, aceita somente arquivos cuja extensão é `.csv` sem diferenciar maiúsculas/minúsculas e ordena pelo nome.

A chamada aparece em `load_bronze_data()`:

```python
input_files = discover_input_files(inbox_path)
```

Essa chamada altera a primeira parte da função:

```text
ANTES
um path conhecido

DEPOIS
uma coleção de paths encontrados
```

Mas a mudança ainda não está completa.

O código preserva:

```python
LEGACY_SOURCE_FILE = "logs_rastreador_2026-07-01.csv"
```

Depois chama `resolve_current_source_file()`.

A própria função documenta que a Silver ainda depende do arquivo legado e, por isso, somente esse arquivo continua alimentando a Bronze naquele estágio.

Por fim, a escrita ainda é:

```text
pd.read_csv()
↓
write_deltalake()
↓
mode="overwrite"
```

Logo, este commit **não deve ser descrito como “a Bronze já processa múltiplos arquivos”**.

O que realmente existe é:

```text
inbox
↓
descoberta de N arquivos
↓
listagem de N arquivos
↓
seleção do arquivo legado
↓
somente ele é escrito
```

Essa distinção é uma evidência importante da evolução gradual.

---

# Passo 13 — Os arquivos descobertos passam a ser validados individualmente

**Commit relacionado:** `190d54a`

# O que?

Surge:

```python
@dataclass(frozen=True)
class FileValidationResult:
```

O objeto carrega o resultado da validação de um arquivo.

# Para que?

Descobrir vários arquivos cria uma nova possibilidade:

```text
A.csv válido
B.csv quebrado
C.csv válido
```

Se a leitura fosse executada diretamente no loop, `B.csv` poderia interromper todo o processamento.

O pipeline precisa separar:

```text
arquivo inválido
```

de:

```text
falha global da execução
```

# Como?

Primeiro é definido o contrato estrutural:

```python
EXPECTED_COLUMNS = (
```

seguido pelas colunas do protocolo esperadas pela Silver.

A validação permite colunas extras, mas considera a ausência de uma coluna esperada um problema estrutural.

A leitura passa por `read_csv_with_supported_encoding()`.

Ela tenta primeiro UTF-8 com BOM e, se a decodificação falhar, tenta Latin-1.

Depois cada arquivo passa por:

```python
result = validate_input_file(file_path)
```

dentro de um loop independente.

A consequência imediata é verificada:

```python
if result.is_valid:
```

Se verdadeiro, o loop usa `continue`.

Se falso, a execução não lança imediatamente uma exceção global: o arquivo segue para isolamento.

A movimentação começa em:

```python
move_file_to_quarantine(...)
```

O código executa um `shutil.move()` para o diretório de quarentena e cria ao lado um arquivo:

```text
<arquivo>.error.txt
```

O relatório registra informações como:

```text
arquivo
horário
motivo
colunas ausentes
encoding
quantidade de linhas
```

Assim:

```text
arquivo descoberto
↓
validate_input_file()
├── válido
│   ↓
│   permanece candidato ao processamento
│
└── inválido
    ↓
    quarantine/
    ↓
    relatório .error.txt
```

Mas ainda existe uma limitação histórica.

Depois da validação aparece `find_valid_legacy_source()`.

Ou seja:

```text
N arquivos são validados
↓
somente o legado válido alimenta a Bronze
```

A persistência ainda usa `overwrite`.

---

# Passo 14 — As linhas recebem identidade técnica e linhagem

**Commit relacionado:** `fd8bc7f`

# O que?

É criado um conjunto explícito de metadados:

```python
METADATA_COLUMNS = (
    "source_file",
    "source_file_hash",
    ...
)
```

As linhas passam a carregar:

```text
source_file
source_file_hash
source_row_number
row_id
batch_id
ingested_at
ingestion_date
```

# Para que?

Quando vários arquivos começarem a ser consolidados, o path da tabela não será mais suficiente para descobrir a origem de uma linha.

O dado precisa responder por si mesmo:

```text
de onde vim?
em qual posição?
em qual execução?
quando fui ingerido?
qual é minha identidade estável?
```

# Como?

O fluxo começa pela identidade do arquivo.

`calculate_file_hash()` usa SHA-256 sobre o conteúdo binário.

Portanto:

```text
mesmo conteúdo
↓
mesmo source_file_hash
```

independentemente do nome da execução.

Depois o batch recebe um UUID através de:

```python
return str(uuid4())
```

O batch representa a execução, não a identidade permanente da linha.

Essa diferença fica explícita em `calculate_row_id()`.

A identidade utilizada é derivada de:

```text
source_file_hash
:
source_row_number
```

O comentário histórico deixa explícito que `batch_id` não participa do cálculo.

Logo:

```text
mesmo arquivo
+
mesma linha
+
batch diferente
↓
mesmo row_id
```

Depois `prepare_bronze_dataframe()` aplica esses metadados ao DataFrame. Na versão consolidada posterior, a sequência pode ser observada diretamente:

```python
bronze_dataframe["source_file"] = source_path.name
```

e, logo depois, são preenchidos hash, posição, `row_id`, batch e timestamps.

A transformação do dado agora é:

```text
linha original do CSV
↓
source_file
↓
source_file_hash
↓
source_row_number
↓
row_id
↓
batch_id
↓
ingested_at
↓
ingestion_date
```

Neste commit, porém, adicionar a identidade **ainda não significa que ela já esteja sendo usada em um MERGE consolidado**.

Essa etapa virá depois.

---

# Passo 15 — Surge a tabela de controle, mas o processamento múltiplo ainda permanece parcialmente desativado

**Commit relacionado:** `6240762`

# O que?

É criado:

```text
data/lakehouse/00_control/ingestion_files
```

e surge:

```python
@dataclass(frozen=True)
class IngestionControlEvent:
```

# Para que?

Até aqui o sistema consegue identificar dados, mas ainda não possui memória operacional de cada tentativa.

É necessário distinguir:

```text
começou
terminou
falhou
foi ignorado
```

# Como?

Os estados admitidos são definidos explicitamente:

```python
CONTROL_STATUSES = (
    "PROCESSING",
    "SUCCESS",
    "FAILED",
    "SKIPPED",
)
```

`IngestionControlEvent` inclui o batch, arquivo, hash, status, timestamps e contagens.

A tabela não atualiza o registro anterior.

`append_control_event()` escolhe:

```text
append, se a Delta Table já existe
overwrite, somente para sua criação
```

e grava um novo evento.

Isso materializa:

```text
tentativa
↓
PROCESSING
↓
nova linha

conclusão
↓
SUCCESS
↓
outra linha
```

Não:

```text
PROCESSING
↓
UPDATE para SUCCESS
```

`load_successful_file_hashes()` lê posteriormente apenas eventos cujo status é `SUCCESS`.

O hash passa então a responder:

```text
este conteúdo já teve uma ingestão concluída?
```

Mas existe novamente um estado intermediário muito importante.

O próprio `process_input_files_with_control()` dessa versão documenta que somente o arquivo legado é escrito na Bronze; outros arquivos válidos recebem `SKIPPED` e permanecem na inbox.

Portanto a evolução real é:

```text
descoberta múltipla
↓
validação múltipla
↓
linhagem
↓
controle por arquivo
↓
AINDA sem Bronze múltipla consolidada
```

Isso prepara o commit seguinte.

---

# Passo 16 — `tracker_logs` elimina a Bronze por arquivo

**Commit relacionado:** `0893638`

# O que?

A Bronze passa a ter um destino único:

```text
01_bronze/tracker_logs
```

Em vez de:

```text
arquivo A → tabela A
arquivo B → tabela B
```

o modelo passa a ser:

```text
arquivo A ┐
arquivo B ├→ tracker_logs
arquivo C ┘
```

# Para que?

A Silver não deve precisar descobrir uma quantidade crescente de tabelas Bronze.

Além disso, a Bronze agora já possui:

```text
row_id
batch_id
hash
controle
```

o que permite consolidar os fatos sem depender do path do arquivo.

# Como?

O path único é centralizado por `get_bronze_table_path()`.

Na versão atual, ele termina em:

```text
01_bronze / BRONZE_TABLE_NAME
```

e `BRONZE_TABLE_NAME` é `tracker_logs`.

Quando a tabela ainda não existe, `write_bronze_table()` cria a Delta Table particionada por `ingestion_date`.

A mudança principal acontece nas execuções seguintes.

A tabela existente é aberta:

```python
bronze_table = DeltaTable(str(bronze_path))
```

Depois o DataFrame é alinhado ao schema existente para adicionar colunas antigas ausentes como `NULL` sem descartar novas colunas da fonte.

A comparação começa no `MERGE`.

O predicado real é:

```python
predicate="target.row_id = source.row_id"
```

Essa linha transforma `row_id` em algo além de metadado de auditoria.

Ele passa a ser a chave da idempotência.

A política do `MERGE` contém apenas:

```python
.when_not_matched_insert_all()
```

Não há cláusula de update.

Logo:

```text
row_id encontrado
↓
MATCH
↓
nenhuma alteração

row_id não encontrado
↓
NOT MATCHED
↓
INSERT
```

Depois da execução, as métricas do `MERGE` são consultadas para obter o número de linhas efetivamente inseridas.

A quantidade de duplicadas é derivada de:

```text
linhas recebidas - linhas inseridas
```

A Bronze passa, portanto, a possuir semântica **insert-only**.

Isso corresponde ao tipo de informação armazenado:

```text
tracker_logs
↓
fato ingerido
↓
não representa estado mutável
```

---

# Passo 17 — Um `SUCCESS` só é confiável se o dado também existir na Bronze

**Commit principal:** `0893638`

# O que?

A lógica de skip deixa de confiar apenas na tabela de controle.

Ela cruza:

```text
SUCCESS no control
∩
hashes presentes em tracker_logs
```

# Para que?

Depois da migração, um `SUCCESS` histórico pode ter sido produzido em uma versão anterior que gravava outra estrutura Bronze.

Se apenas o control fosse consultado:

```text
SUCCESS antigo
↓
skip
↓
tracker_logs pode continuar sem o arquivo
```

# Como?

A versão atual carrega os dois conjuntos separadamente:

```python
control_success_hashes = load_successful_file_hashes(...)
```

e:

```python
bronze_hashes = load_ingested_file_hashes(...)
```

Depois faz a interseção:

```python
successful_file_hashes = control_success_hashes & bronze_hashes
```

Somente os hashes presentes nos dois lados são tratados como concluídos.

O processamento segue então para:

```python
process_input_files_with_control(...)
```

Dentro desse ciclo:

```text
arquivo
↓
hash
↓
hash confirmado?
├── sim → SKIPPED
└── não
    ↓
    PROCESSING
    ↓
    validação
```

Se inválido:

```text
FAILED
↓
quarantine
```

Se válido:

```text
prepare_bronze_dataframe()
↓
MERGE
↓
SUCCESS
```

Depois do `SUCCESS`, aparece:

```python
try_move_file_to_archive(...)
```

Essa ordem é importante.

O dado é primeiro commitado e registrado como sucesso; somente depois o arquivo físico é arquivado.

A função de archive também evita transformar uma ingestão já commitada em `FAILED` caso apenas a movimentação do arquivo falhe.

A Bronze agora possui o ciclo operacional completo:

```text
inbox
↓
controle
↓
validação
↓
Delta
↓
SUCCESS
↓
archive
```

---

# Parte V — A Silver precisa acompanhar a nova Bronze

# Passo 18 — A Silver deixa de ler uma Bronze por arquivo

**Commit relacionado:** `c46d195`

# O que?

A Silver define:

```python
BRONZE_TABLE_NAME = "tracker_logs"
```

e passa a declarar formalmente as colunas de linhagem que espera receber.

# Para que?

Depois da consolidação, a antiga suposição:

```text
nome do arquivo
≈
nome da Bronze
```

deixa de existir.

Além disso, a Silver não deve voltar a fabricar `source_file` a partir do path, porque a origem já foi preservada linha a linha.

# Como?

O path passa a terminar em:

```text
01_bronze/tracker_logs
```

A tabela é carregada por `load_bronze_table()`.

Antes de qualquer SQL de negócio, o código executa:

```python
validate_bronze_metadata(bronze_table)
```

Essa função obtém os nomes das colunas e calcula as ausentes em relação a:

```text
source_file
source_file_hash
source_row_number
row_id
batch_id
ingested_at
ingestion_date
```

Se alguma estiver faltando, a Silver falha explicitamente.

A consequência é um contrato real:

```text
Bronze compatível
↓
possui linhagem completa
↓
Silver pode começar

Bronze incompatível
↓
ValueError
↓
não esconder perda de linhagem
```

As queries Silver passam a carregar os metadados até os produtos.

No caminho T1, por exemplo, `source_file`, hash, número da linha, `row_id`, batch e timestamps de ingestão seguem dentro da seleção que produz `device_identity_events`.

Mas esse commit ainda deixa uma limitação explícita.

`write_silver_table()` usa:

```text
mode="overwrite"
schema_mode="overwrite"
```

e o comentário informa que a Silver ainda faz rebuild completo.

Logo:

```text
Bronze
incremental

↓

Silver
FULL

↓

Gold
FULL
```

A Bronze já sabe exatamente o que chegou, mas essa informação ainda não restringe o trabalho da Silver.

---

# Passo 19 — A assinatura da Silver passa a aceitar contexto incremental

**Commit relacionado:** `7990864`

# O que?

Surge:

```python
@dataclass(frozen=True)
class SilverLoadResult:
```

O objeto contém:

```text
mode
batch_ids
affected_event_dates
affected_rejection_dates
contagens de escrita
```

A função também passa a trabalhar com `batch_ids`.

# Para que?

O problema da versão anterior é:

```text
1 arquivo novo
↓
Bronze sabe qual batch o inseriu
↓
Silver ignora essa informação
↓
recalcula tudo
```

`batch_ids` cria a ponte entre:

```text
o que acabou de entrar
```

e:

```text
qual região lógica precisa ser reconstruída
```

# Como?

A primeira etapa não é consultar datas.

Os batches são normalizados:

```python
normalize_batch_ids(batch_ids)
```

Essa função remove valores vazios, elimina repetições e ordena os identificadores.

Depois eles são registrados no DuckDB como uma pequena tabela auxiliar.

A query de `discover_affected_partitions()` faz join entre:

```text
bronze.batch_id
```

e:

```text
requested_silver_batches.batch_id
```

Só então tenta determinar o timestamp:

```text
TM_STAMP
↓ se inválido
DATA_SERVIDOR
```

usando `COALESCE(TRY_CAST(...), TRY_CAST(...))`.

A data extraída desse timestamp torna-se:

```text
affected_event_dates
```

Depois uma segunda consulta procura linhas dos mesmos batches cujo timestamp continue `NULL`.

Se existirem:

```text
affected_rejection_dates
=
event_dates + "unknown"
```

Portanto:

```text
batch_ids
↓
linhas que chegaram nesses batches
↓
timestamps
↓
datas
↓
event_dates
+
rejection_dates
```

O batch não é ainda a unidade de escrita.

Ele serve para descobrir o impacto.

---

# Passo 20 — A Silver deliberadamente abandona o batch depois de descobrir a data

**Commit relacionado:** `7990864`

# O que?

A função `create_bronze_scope_view()` não mantém a Silver restrita às linhas dos novos batches.

# Para que?

Esse é o ponto central do late-arriving data.

Considere uma partição Silver já contendo:

```text
10:00
10:10
10:20
```

Depois chega:

```text
10:15
```

Se a Silver reconstruísse a partição apenas com a linha do novo batch:

```text
nova partição
=
10:15
```

os registros antigos seriam perdidos.

Se simplesmente anexasse o evento, a operação deixaria de ser uma reconstrução determinística da partição.

# Como?

O próprio docstring da função registra a decisão:

```text
no modo incremental,
NÃO filtrar somente os novos batches
```

Depois de descobrir as datas, a função cria uma tabela auxiliar com:

```text
affected_event_dates
```

e constrói `bronze_scope`.

A condição final aceita linhas cujo timestamp pertença às datas afetadas.

Se `unknown` estiver envolvido, inclui também registros cujo timestamp não pôde ser determinado.

O comportamento efetivo é:

```text
batch novo
↓
descobrir 2026-08-10
↓
batch deixa de ser filtro principal
↓
buscar TODA a Bronze de 2026-08-10
↓
reaplicar transformação Silver
```

Essa é a base real da correção de late-arriving data.

---

# Passo 21 — A Silver deixa de sobrescrever a tabela e passa a substituir partições específicas

**Commit relacionado:** `7990864`

# O que?

No modo incremental, a escrita passa a utilizar um predicate correspondente à partição que está sendo reconstruída.

# Para que?

Depois de reler somente as datas afetadas, não faria sentido substituir:

```text
todas as datas Silver
```

O escopo da leitura e o escopo da escrita precisam ser equivalentes.

# Como?

Para cada valor afetado, a função constrói conceitualmente:

```text
event_date = '2026-08-10'
```

ou:

```text
rejection_date = 'unknown'
```

Depois a escrita utiliza:

```text
mode = overwrite
+
predicate = partição atual
```

A função `write_incremental_silver_partitions()` mantém uma `pa.Table` e filtra cada partição antes de entregá-la ao Delta.

Assim:

```text
bronze_scope = data afetada completa
↓
telemetry / identity / rejected
↓
Arrow
↓
separar partição
↓
write_deltalake(overwrite + predicate)
↓
somente a partição é substituída
```

A operação continua sendo `overwrite`, mas o significado mudou:

```text
ANTES
overwrite da tabela

DEPOIS
overwrite de uma região selecionada
```

---

# Passo 22 — O processamento incremental revela um bug de schema que o full rebuild escondia

**Commit relacionado:** `7990864`

# O que?

PyArrow passa a ser usado diretamente na escrita das partições Silver.

# Para que?

Uma partição pequena pode possuir uma coluna totalmente nula:

```text
NULL
NULL
NULL
```

Se o schema for reinferido apenas a partir desses valores, uma coluna que semanticamente deveria continuar `string` pode virar `Null`.

Mais tarde chega:

```text
"valor"
```

e a Delta Table precisa conciliar:

```text
Null
versus
String
```

Esse cenário apareceu especialmente no processamento de rejeitados e na partição `unknown`.

# Como?

A versão incremental importa:

```python
import pyarrow as pa
import pyarrow.compute as pc
```

Em vez de converter cada subconjunto novamente para Pandas, `filter_arrow_partition()` recebe uma `pa.Table` já tipada.

Ela cria uma máscara Arrow e retorna:

```text
table.filter(mask)
```

preservando o schema original da tabela.

A sequência corrigida torna-se:

```text
DuckDB produz resultado
↓
fetch_arrow_table()
↓
schema Arrow
↓
filtrar partição sem reinferir tipos
↓
write_deltalake()
```

Portanto, a correção não é simplesmente:

```text
"adicionar PyArrow"
```

O que ela realmente muda é:

```text
ANTES
subconjunto
↓
nova inferência de schema
↓
possível NullType

DEPOIS
tabela Arrow tipada
↓
filtro preserva schema
↓
Delta recebe o tipo esperado
```

---

# Passo 23 — A Silver passa a possuir um retorno explícito de `NOOP`

**Commit relacionado:** `7990864`

# O que?

`SilverLoadResult.mode` pode ser:

```text
NOOP
```

# Para que?

Um `batch_id` informado não garante que existam linhas correspondentes na Bronze.

Sem uma condição explícita, a Silver poderia executar transformações e escritas que não alteram nada.

# Como?

Depois de `discover_affected_partitions()`:

```text
affected_event_dates = ()
affected_rejection_dates = ()
```

leva a um retorno antecipado de `SilverLoadResult` com contagens zeradas.

Isso transforma:

```text
"não encontrei nada"
```

em um resultado formal que o pipeline poderá interpretar.

O `NOOP` deixa de ser apenas ausência de efeito e vira informação de controle.

---

# Parte VI — A Gold passa a ser incremental segundo a natureza de cada produto

# Passo 24 — A Gold recebe explicitamente datas afetadas

**Commit relacionado:** `ffc6b41`

# O que?

Surge:

```python
@dataclass(frozen=True)
class GoldLoadResult:
```

A função `load_gold_data()` recebe:

```text
affected_event_dates
affected_rejection_dates
```

# Para que?

A Silver já consegue responder:

```text
quais datas mudaram?
```

Portanto a Gold não precisa recalcular indiscriminadamente datas não afetadas.

Mas os cinco produtos Gold não podem ser atualizados da mesma maneira.

# Como?

Primeiro as datas são normalizadas.

Depois o código identifica se a chamada solicitou processamento incremental.

Se foram fornecidas coleções, mas ambas resultaram vazias, a função retorna imediatamente:

```text
GoldLoadResult(mode="NOOP")
```

Se houver impacto, `gold_supports_incremental_update()` verifica se as cinco tabelas já existem e se as tabelas temporais estão particionadas pelas colunas e tipos esperados.

Se a estrutura antiga não for compatível:

```text
incremental solicitado
+
Gold antiga
↓
full rebuild de migração
```

Somente depois o modo incremental pode ser utilizado com segurança.

---

# Passo 25 — Datas afetadas são convertidas em dispositivos afetados

**Commit relacionado:** `ffc6b41`

# O que?

A Gold cria:

```python
discover_affected_devices(...)
```

# Para que?

Produtos como:

```text
dim_device
device_last_position
```

não são particionados semanticamente por data.

Eles representam uma entidade.

Se `2026-08-10` mudou, a pergunta seguinte é:

```text
quais device_serial aparecem nessa mudança?
```

# Como?

A função recebe `event_dates`.

Ela registra as datas em DuckDB e consulta tanto:

```text
telemetry_gold_base
```

quanto:

```text
identity_gold_base
```

A consulta retorna `DISTINCT device_serial`.

Portanto:

```text
affected_event_dates
↓
Silver rows dessas datas
↓
device_serial distintos
↓
affected_devices
```

Com isso a Gold já possui dois tipos de escopo:

```text
datas
→ produtos particionados

devices
→ produtos de estado
```

---

# Passo 26 — `dim_device` e `device_last_position` usam upsert

**Commit relacionado:** `ffc6b41`

# O que?

As duas tabelas de estado passam por:

```python
merge_entity_table(...)
```

# Para que?

Uma linha de `dim_device` não representa um fato imutável.

Ela pode mudar quando chega uma identidade mais nova.

Da mesma forma, `device_last_position` precisa substituir o estado anterior se um evento mais recente alterar a posição conhecida.

# Como?

A tabela Delta existente é aberta.

O `MERGE` compara a chave fornecida.

Na chamada real:

```text
key = "device_serial"
```

A política contém as duas ramificações:

```python
.when_matched_update_all()
.when_not_matched_insert_all()
```

A consequência é:

```text
device_serial encontrado
↓
UPDATE

device_serial novo
↓
INSERT
```

Esse comportamento é diferente da Bronze.

## Bronze

```text
row_id encontrado
↓
não alterar
```

## Gold de estado

```text
device_serial encontrado
↓
atualizar estado
```

A diferença vem do que cada tabela representa.

---

# Passo 27 — Rotas e agregações continuam sendo reconstruídas como conjuntos

**Commit relacionado:** `ffc6b41`

# O que?

As tabelas:

```text
device_route_points
device_daily_summary
data_quality_summary
```

não usam o mesmo upsert por entidade.

Elas passam por:

```python
replace_partitions(...)
```

# Para que?

Uma rota é dependente da sequência completa do dia.

Um resumo diário depende da totalidade dos eventos do dia.

Qualidade depende das contagens completas da data.

Atualizar apenas uma linha nova isoladamente pode produzir um resultado derivado incorreto.

# Como?

No ramo incremental de `load_gold_data()`:

```text
build_route_points(event_dates)
build_daily_summary(event_dates)
build_quality_summary(quality_dates)
```

são chamados apenas para as datas afetadas.

Depois:

```text
route_points
→ replace_partitions(event_date)

daily_summary
→ replace_partitions(event_date)

quality_summary
→ replace_partitions(metric_date)
```

Dentro de `replace_partitions()`, o predicate é construído para um único valor de partição.

A escrita utiliza explicitamente:

```text
mode="overwrite"
predicate=<partição>
```

Portanto:

```text
2026-08-10 mudou
↓
recalcular resultado completo de 2026-08-10
↓
substituir 2026-08-10

2026-08-11 não mudou
↓
não tocar
```

---

# Passo 28 — Uma partição que deixa de produzir linhas precisa ser apagada

**Commit relacionado:** `ffc6b41`

# O que?

`replace_partitions()` possui um ramo específico quando:

```text
partition_table.num_rows == 0
```

# Para que?

Considere:

```text
Gold antiga
unknown → 1 registro
```

Depois da reconstrução da Silver:

```text
Gold recalculada
unknown → 0 registros
```

Se a Gold simplesmente não escrever nada, a linha antiga continua fisicamente presente.

O resultado se tornaria obsoleto.

# Como?

A função verifica se o subconjunto está vazio.

Quando está, chama:

```python
DeltaTable(str(path)).delete(predicate)
```

Caso haja linhas, o outro ramo chama `write_deltalake()` com overwrite seletivo.

Assim:

```text
partição recalculada
↓
tem linhas?
├── sim
│   ↓
│   overwrite seletivo
│
└── não
    ↓
    DELETE da partição anterior
```

Essa operação é necessária porque incrementalidade não significa apenas adicionar ou atualizar.

Às vezes o novo estado correto é:

```text
não existir mais
```

---

# Passo 29 — A Gold constrói um objeto que descreve exatamente o que foi alterado

**Commit relacionado:** `ffc6b41`

# O que?

Ao terminar, `load_gold_data()` cria:

```python
result = GoldLoadResult(
```

# Para que?

Assim como Silver e Bronze, a Gold deixa de comunicar somente:

```text
"terminei"
```

e passa a comunicar:

```text
como processei?
quais datas?
quais devices?
quantas linhas de cada produto?
```

# Como?

No modo incremental, o código primeiro executa:

```text
discover_affected_devices()
```

Depois constrói as cinco tabelas de saída em escopos diferentes.

Em seguida escolhe:

```text
MERGE
para estado
```

e:

```text
replace_partitions
para resultados temporais
```

Finalmente `GoldLoadResult` recebe:

```text
mode
affected_event_dates
affected_rejection_dates
affected_devices
rows_written de cada produto
```

O objeto representa, portanto, o efeito efetivo da etapa.

---

# Parte VII — A Bronze passa a informar ao pipeline se realmente mudou

# Passo 30 — `BronzeLoadResult` diferencia execução de mudança real

**Commit relacionado:** `ffc6b41`

# O que?

É criado:

```python
@dataclass(frozen=True)
class BronzeLoadResult:
```

Os campos principais são:

```text
execution_batch_id
batch_ids
has_new_data
inserted_row_count
source_files
validation_results
```

# Para que?

Existe uma diferença entre:

```text
arquivo processado com sucesso
```

e:

```text
arquivo inseriu dados novos
```

Um retry pode terminar em `SUCCESS`, mas o `MERGE` encontrar todos os `row_id`.

Nesse caso:

```text
SUCCESS
+
0 inserts
```

não deve disparar Silver e Gold.

# Como?

Depois de processar os arquivos, `load_bronze_data()` não depende apenas das métricas de controle.

Ela chama:

```python
summarize_inserted_batch(...)
```

Essa função abre `tracker_logs` e aplica um filtro físico por:

```text
batch_id = batch atual
```

Se nenhuma linha da Bronze possuir aquele batch:

```text
return 0, ()
```

Logo, o batch executou, mas não deixou fatos novos.

Depois:

```text
inserted_row_count > 0
```

controla dois campos diferentes.

Quando verdadeiro:

```text
batch_ids = (batch_id,)
has_new_data = True
```

Quando falso:

```text
batch_ids = ()
has_new_data = False
```

Assim, `batch_ids` deixa de significar:

```text
"batches que foram tentados"
```

e passa a significar:

```text
"batches desta execução que efetivamente deixaram linhas novas na Bronze"
```

---

# Passo 31 — Inbox vazia vira `NOOP` antes mesmo de consultar Silver

**Commit relacionado:** `ffc6b41`

# O que?

`load_bronze_data()` possui retorno antecipado quando:

```python
if not input_files:
```

# Para que?

Sem arquivos:

```text
não há validação
não há MERGE
não há batch relevante para Silver
```

# Como?

O retorno já constrói:

```text
batch_ids = ()
has_new_data = False
inserted_row_count = 0
```

A ausência de trabalho deixa de depender de inferência do pipeline.

A Bronze comunica explicitamente:

```text
não houve mudança
```

---

# Parte VIII — O pipeline passa de sequência fixa para propagação de impacto

# Passo 32 — `PipelineResult` formaliza o estado das três camadas

**Commit relacionado:** `ffc6b41`
**Arquivo:** `src/lakehouse_pipeline.py`

# O que?

O pipeline ganha:

```python
@dataclass(frozen=True)
class PipelineResult:
```

Ele carrega:

```text
status
bronze
silver
gold
```

# Para que?

Compare o contrato anterior:

```text
função Bronze terminou
↓
executar função Silver
```

com o novo:

```text
BronzeLoadResult
↓
interpretar resultado
↓
decidir se Silver é necessária
```

A camada anterior passa a dirigir a próxima.

# Como?

A primeira chamada agora é armazenada:

```python
bronze_result = load_bronze_data()
```

Logo em seguida aparece a condição:

```python
if not bronze_result.has_new_data:
```

Se verdadeira:

```text
silver = None
gold = None
status = NOOP
```

e a função retorna.

Portanto Silver nem é chamada.

Se houver dados novos, o próximo trecho consome diretamente o objeto anterior:

```python
batch_ids=bronze_result.batch_ids
```

Isso materializa a primeira passagem de contexto:

```text
Bronze
↓
batch_ids efetivamente inseridos
↓
Silver
```

A Silver retorna `SilverLoadResult`.

O pipeline então verifica:

```python
if silver_result.mode == "NOOP":
```

Se verdadeiro, Gold novamente não é executada.

Quando Silver produziu impacto, as datas são passadas diretamente:

```text
silver_result.affected_event_dates
silver_result.affected_rejection_dates
↓
load_gold_data()
```

Finalmente:

```text
BronzeLoadResult
+
SilverLoadResult
+
GoldLoadResult
↓
PipelineResult
```

O pipeline atual pode ser representado somente depois de percorrer esse código:

```text
load_bronze_data()
↓
BronzeLoadResult
↓
has_new_data?
├── não → NOOP
└── sim
    ↓
    batch_ids
    ↓
    load_silver_data()
    ↓
    SilverLoadResult
    ↓
    mode == NOOP?
    ├── sim → SILVER_NOOP
    └── não
        ↓
        affected_event_dates
        affected_rejection_dates
        ↓
        load_gold_data()
        ↓
        GoldLoadResult
        ↓
        PipelineResult(SUCCESS)
```

A transformação central da orquestração é, portanto:

```text
ANTES
controle de ordem

DEPOIS
controle de ordem
+
propagação de impacto
+
short-circuit
```

---

# Passo 33 — Late-arriving data funciona como consequência da cadeia completa

**Commits principais:** `7990864` + `ffc6b41`

Este comportamento não deve ser explicado apenas conceitualmente. Ele pode ser percorrido pelas funções que acabaram de ser construídas.

## Estado existente

Suponha que `device_route_points` possua:

```text
10:00 → point_sequence 1
10:10 → point_sequence 2
10:20 → point_sequence 3
```

A sequência vem do `ROW_NUMBER()` ordenado dentro de `device_serial + event_date`.

## Chega `10:15`

### 1. Bronze identifica o arquivo

```text
calculate_file_hash()
↓
validate_input_file()
↓
prepare_bronze_dataframe()
```

A linha recebe `source_file_hash`, `source_row_number`, `row_id` e `batch_id`.

### 2. Bronze executa o `MERGE`

```text
target.row_id = source.row_id
```

Como o evento é novo:

```text
NOT MATCHED
↓
INSERT
```

### 3. Bronze confirma que o batch deixou linhas

```text
summarize_inserted_batch()
↓
inserted_row_count > 0
↓
batch_ids = (batch_id,)
```

### 4. Pipeline entrega o batch à Silver

```text
load_silver_data(
    batch_ids=bronze_result.batch_ids
)
```

### 5. Silver descobre a data

`discover_affected_partitions()` encontra o timestamp `10:15` no batch e deriva:

```text
affected_event_dates
=
2026-08-10
```

### 6. Silver relê a data inteira

`create_bronze_scope_view()` abandona o filtro por batch e seleciona a Bronze completa de `2026-08-10`.

Agora o escopo contém:

```text
10:00
10:10
10:15
10:20
```

### 7. Silver substitui a partição

As três transformações são reaplicadas e somente a região correspondente à data afetada é substituída.

### 8. Pipeline entrega a data à Gold

```text
affected_event_dates
↓
load_gold_data()
```

### 9. Gold reconstrói a rota

`build_route_points()` recebe `event_dates` e reordena os pontos daquele dia.

A nova sequência passa a ser:

```text
10:00 → 1
10:10 → 2
10:15 → 3
10:20 → 4
```

### 10. Gold substitui a partição

`replace_partitions()` escreve somente:

```text
event_date = 2026-08-10
```

O comportamento de late-arriving data é, portanto, consequência direta desta cadeia:

```text
novo fato
↓
batch
↓
data impactada
↓
releitura da partição completa
↓
Silver reconstruída
↓
Gold reconstruída
↓
sequência corrigida
```

---

# Evolução das estratégias de escrita

A história do projeto também pode ser reconstruída apenas acompanhando as operações de persistência.

## 1. Data Lake Bronze

```python
df.to_parquet(...)
```

```text
DataFrame
↓
Parquet
```

---

## 2. Data Lake Silver/Gold

```text
DuckDB
↓
COPY / Parquet
↓
full refresh
```

---

## 3. Primeiro Lakehouse

```text
write_deltalake
↓
mode = overwrite
```

Delta existe, mas a reconstrução continua integral.

---

## 4. Bronze consolidada

```text
MERGE
ON row_id
↓
WHEN NOT MATCHED → INSERT
```

Sem update.

Uso:

```text
fato ingerido
```

---

## 5. Silver incremental

```text
overwrite
+
predicate de event_date/rejection_date
```

Uso:

```text
reconstruir somente partição afetada
```

---

## 6. Gold de estado

```text
MERGE por device_serial
↓
MATCH → UPDATE
NOT MATCHED → INSERT
```

Uso:

```text
dim_device
device_last_position
```

---

## 7. Gold por data

```text
overwrite + predicate
```

Uso:

```text
device_route_points
device_daily_summary
data_quality_summary
```

---

## 8. Resultado derivado que deixa de existir

```text
num_rows == 0
↓
DELETE predicate
```

A evolução completa fica:

```text
Parquet overwrite
↓
Delta overwrite
↓
MERGE insert-only
↓
overwrite seletivo
↓
MERGE UPDATE + INSERT
↓
DELETE seletivo
```

---

# Síntese das funções centrais

## Bronze

```text
load_bronze_data()
```

começa como:

```text
path fixo
↓
pd.read_csv
↓
to_parquet
```

Depois passa por:

```text
create_raw_directories()
↓
discover_input_files()
```

mas ainda preserva:

```text
LEGACY_SOURCE_FILE
```

Depois recebe:

```text
validate_input_file()
↓
FileValidationResult
↓
quarantine
```

Depois:

```text
calculate_file_hash()
calculate_row_id()
prepare_bronze_dataframe()
```

Depois:

```text
IngestionControlEvent
↓
PROCESSING / SUCCESS / FAILED / SKIPPED
```

Depois:

```text
tracker_logs
↓
write_bronze_table()
↓
MERGE row_id insert-only
```

Finalmente:

```text
summarize_inserted_batch()
↓
BronzeLoadResult
```

---

# Silver

A Silver começa como:

```text
Parquet Bronze
↓
bronze_normalized
↓
DuckDB
├── telemetry_events
├── device_identity_events
└── rejected_logs
```

Depois passa a validar:

```text
BRONZE_METADATA_COLUMNS
```

contra `tracker_logs`.

Em seguida recebe:

```text
batch_ids
↓
normalize_batch_ids()
↓
discover_affected_partitions()
```

Depois:

```text
create_bronze_scope_view()
↓
todas as linhas Bronze das datas afetadas
```

e não apenas os batches.

Finalmente:

```text
Arrow
↓
replace seletivo
↓
SilverLoadResult
```

---

# Gold

Começa como:

```text
Silver completa
↓
telemetry_gold_base
identity_gold_base
↓
produtos
↓
full refresh
```

Depois ganha:

```text
device_route_points
↓
point_sequence
```

Finalmente:

```text
affected_event_dates
↓
discover_affected_devices()
```

e divide as estratégias:

```text
dim_device
device_last_position
↓
MERGE

device_route_points
device_daily_summary
data_quality_summary
↓
replace_partitions()
```

---

# Pipeline

## Primeira forma verificável

```text
load_bronze_data()
↓
load_silver_data()
↓
load_gold_data()
```

## Forma atual

```text
BronzeLoadResult
↓
has_new_data
↓
batch_ids
↓
SilverLoadResult
↓
affected dates
↓
GoldLoadResult
↓
PipelineResult
```

A evolução é:

```text
sequenciamento
↓
sequenciamento orientado pelo impacto
```

---

# Grandes versões arquiteturais resultantes

## Versão 1 — Data Lake local em Parquet

```text
CSV
↓
Bronze Parquet
↓
Silver Parquet
↓
Gold Parquet
```

Commits centrais:

```text
110f9cc
2d1d9c4
fba59ea
0d900ac
```

---

## Versão 2 — Lakehouse Delta ainda integral

```text
CSV fixo
↓
Bronze Delta overwrite
↓
Silver Delta overwrite
↓
Gold Delta overwrite
```

Commit central:

```text
c9d42c7
```

---

## Versão 3 — Lakehouse exposto por API

```text
Gold
↓
device_route_points
↓
FastAPI
```

Commits:

```text
58acdfd
7bc4f03
```

---

## Versão 4 — Ingestão preparada para múltiplos arquivos

```text
inbox
↓
discover_input_files
↓
validação
↓
quarantine
↓
linhagem
↓
controle
```

Mas é importante preservar o fato histórico:

```text
descoberta múltipla
≠
imediatamente escrita múltipla
```

A dependência do arquivo legado só é eliminada na consolidação posterior.

---

## Versão 5 — Bronze consolidada e idempotente

```text
N arquivos
↓
tracker_logs
↓
row_id
↓
MERGE insert-only
```

Commit central:

```text
0893638
```

---

## Versão 6 — Silver orientada por impacto temporal

```text
batch_ids
↓
datas afetadas
↓
releitura das datas
↓
replace seletivo
```

Commit central:

```text
7990864
```

---

## Versão 7 — Incrementalidade ponta a ponta

```text
BronzeLoadResult
↓
SilverLoadResult
↓
GoldLoadResult
↓
PipelineResult
```

com:

```text
insert-only
+
upsert
+
replace seletivo
+
delete seletivo
+
NOOP
```

Commit central:

```text
ffc6b41
```

---

# Estado atual reconstruído

Depois de percorrer a implementação, a arquitetura atual pode ser representada sem esconder de onde cada seta veio:

```text
data/raw/inbox/
↓
discover_input_files()
↓
calculate_file_hash()
↓
controle histórico
↓
validate_input_file()
├── inválido
│   ↓
│   FAILED
│   ↓
│   quarantine
│
└── válido
    ↓
    prepare_bronze_dataframe()
    ↓
    source_file
    source_file_hash
    source_row_number
    row_id
    batch_id
    ingested_at
    ingestion_date
    ↓
    write_bronze_table()
    ↓
    MERGE por row_id
    ↓
    SUCCESS
    ↓
    archive
```

Depois:

```text
summarize_inserted_batch()
↓
BronzeLoadResult
↓
has_new_data?
```

Se falso:

```text
Pipeline NOOP
```

Se verdadeiro:

```text
batch_ids
↓
Silver
↓
discover_affected_partitions()
↓
affected_event_dates
affected_rejection_dates
↓
create_bronze_scope_view()
↓
reler partições completas
↓
telemetry_events
device_identity_events
rejected_logs
↓
replace seletivo
↓
SilverLoadResult
```

Depois:

```text
affected_event_dates
↓
Gold
↓
discover_affected_devices()
```

A Gold divide o impacto:

```text
device_serial
↓
dim_device
device_last_position
↓
MERGE UPDATE + INSERT
```

e:

```text
event_date / metric_date
↓
device_route_points
device_daily_summary
data_quality_summary
↓
replace seletivo
↓
DELETE se resultado ficar vazio
```

Finalmente:

```text
GoldLoadResult
↓
PipelineResult
↓
Gold Delta
↓
lakehouse_reader
↓
FastAPI
```

O caminho histórico completo deixa, portanto, de ser apenas:

```text
CSV
↓
Lakehouse
```

e passa a ser entendido como:

```text
um CSV fixo
↓
Parquet
↓
interpretação do protocolo
↓
produtos analíticos
↓
Delta
↓
API
↓
descoberta de arquivos
↓
validação
↓
linhagem
↓
controle
↓
Bronze consolidada
↓
idempotência
↓
contexto por batch
↓
impacto por data
↓
late-arriving data
↓
rebuild seletivo
↓
Gold por entidade e por partição
↓
orquestração orientada pelo impacto
```

Essa sequência explica não apenas **o que o `local-lakehouse-experiment` possui hoje**, mas **qual trecho de implementação criou cada capacidade e qual limitação anterior tornou essa mudança necessária**.
