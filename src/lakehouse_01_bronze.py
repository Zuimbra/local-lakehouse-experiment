from __future__ import annotations

import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

import pandas as pd
from pandas.errors import EmptyDataError, ParserError


LEGACY_SOURCE_FILE = "logs_rastreador_2026-07-01.csv"

METADATA_COLUMNS = (
    "source_file",
    "source_file_hash",
    "source_row_number",
    "row_id",
    "batch_id",
    "ingested_at",
    "ingestion_date",
)

FILE_HASH_CHUNK_SIZE = 1024 * 1024

CONTROL_STATUSES = (
    "PROCESSING",
    "SUCCESS",
    "FAILED",
    "SKIPPED",
)

CONTROL_STAGE = "BRONZE"

# A Silver atual referencia diretamente todas essas colunas.
# Colunas adicionais são permitidas, mas nenhuma destas pode faltar.
EXPECTED_COLUMNS = (
    "DATA_SERVIDOR",
    "TM_STAMP",
    "TIPO_LOG",
    "MESS_TYPE",
    "REPT_TYPE",
    "PRT_VER",
    "S/N ou IMEI",
    "TERM_STATUS",
    "BAT_VOLT",
    "LOC_STATUS",
    "LAT",
    "LONT",
    "SPEED",
    "DIR",
    "INT_BATT",
    "ODO_TRIP",
    "ODO_TOTAL",
    "HORIMETER",
    "HDOP",
    "MCC",
    "MNC",
    "LAC",
    "CELL_ID",
    "RX_LEVEL",
    "SER_COUNT",
    "TX_TECH",
    "GRP_MSG",
    "IO_STATUS",
    "DRIVER_ID",
    "PASS_ID",
    "RPM",
    "TACHO_SPD",
    "TACHO_ODO",
    "TEMP_1",
    "TEMP_2",
    "TEMP_3",
    "TEMP_4",
)


@dataclass(frozen=True)
class FileValidationResult:
    """
    Resultado da validação estrutural de um arquivo CSV.
    """

    source_path: Path
    is_valid: bool
    row_count: int = 0
    dataframe: pd.DataFrame | None = None
    missing_columns: tuple[str, ...] = ()
    reserved_columns: tuple[str, ...] = ()
    error_message: str | None = None
    detected_encoding: str | None = None


@dataclass(frozen=True)
class IngestionControlEvent:
    """
    Evento imutável do histórico de ingestão de um arquivo.

    A tabela de controle é append-only: cada mudança de estado gera
    uma nova linha, preservando o histórico da tentativa.
    """

    control_event_id: str
    batch_id: str
    source_file: str
    source_file_hash: str | None
    status: str
    stage: str
    started_at: datetime
    finished_at: datetime | None
    row_count: int | None
    inserted_row_count: int | None
    duplicate_row_count: int | None
    status_reason: str | None
    error_message: str | None
    recorded_at: datetime


def create_raw_directories(
    project_dir: Path,
) -> tuple[Path, Path, Path, Path]:
    """
    Cria os diretórios da área de entrada dos dados brutos.
    """
    raw_path = project_dir / "data" / "raw"
    inbox_path = raw_path / "inbox"
    archive_path = raw_path / "archive"
    quarantine_path = raw_path / "quarantine"

    for directory in (
        inbox_path,
        archive_path,
        quarantine_path,
    ):
        directory.mkdir(
            parents=True,
            exist_ok=True,
        )

    return (
        raw_path,
        inbox_path,
        archive_path,
        quarantine_path,
    )


def discover_input_files(
    inbox_path: Path,
) -> list[Path]:
    """
    Encontra somente arquivos CSV diretamente dentro da inbox.

    A ordenação pelo nome torna o processamento determinístico.
    """
    return sorted(
        (
            file_path
            for file_path in inbox_path.iterdir()
            if file_path.is_file()
            and file_path.suffix.lower() == ".csv"
        ),
        key=lambda file_path: file_path.name.lower(),
    )


def print_discovered_files(
    input_files: list[Path],
) -> None:
    """
    Exibe os arquivos encontrados.
    """
    total_files = len(input_files)

    if total_files == 0:
        print(
            "[Lakehouse][Bronze] "
            "Nenhum arquivo CSV encontrado na inbox."
        )
        return

    print(
        "[Lakehouse][Bronze] "
        f"{total_files} arquivo(s) CSV encontrado(s):"
    )

    for position, file_path in enumerate(
        input_files,
        start=1,
    ):
        print(
            "[Lakehouse][Bronze] "
            f"[{position}/{total_files}] {file_path.name}"
        )


def normalize_column_names(
    dataframe: pd.DataFrame,
) -> pd.DataFrame:
    """
    Remove espaços externos dos nomes das colunas.

    O conteúdo das linhas não é normalizado nesta sprint.
    """
    normalized_dataframe = dataframe.copy()
    normalized_dataframe.columns = [
        str(column).strip()
        for column in normalized_dataframe.columns
    ]

    return normalized_dataframe


def read_csv_with_supported_encoding(
    file_path: Path,
) -> tuple[pd.DataFrame, str]:
    """
    Lê o CSV tentando primeiro UTF-8 e depois Latin-1.

    O fallback para Latin-1 ocorre apenas quando UTF-8 não consegue
    decodificar o arquivo.
    """
    try:
        dataframe = pd.read_csv(
            file_path,
            encoding="utf-8-sig",
        )
        return dataframe, "utf-8-sig"

    except UnicodeDecodeError:
        dataframe = pd.read_csv(
            file_path,
            encoding="latin-1",
        )
        return dataframe, "latin-1"


def validate_dataframe_schema(
    dataframe: pd.DataFrame,
) -> tuple[str, ...]:
    """
    Retorna as colunas esperadas que não existem no DataFrame.

    Colunas extras são aceitas, exceto os nomes reservados para
    metadados produzidos pela própria camada Bronze.
    """
    available_columns = set(dataframe.columns)

    return tuple(
        column
        for column in EXPECTED_COLUMNS
        if column not in available_columns
    )


def find_reserved_metadata_columns(
    dataframe: pd.DataFrame,
) -> tuple[str, ...]:
    """
    Impede que o arquivo bruto forneça metadados de linhagem.

    Esses campos precisam ser gerados exclusivamente pelo pipeline.
    """
    available_columns = set(dataframe.columns)

    return tuple(
        column
        for column in METADATA_COLUMNS
        if column in available_columns
    )


def validate_input_file(
    file_path: Path,
) -> FileValidationResult:
    """
    Valida a estrutura de um único CSV.

    Esta função não rejeita linhas por conteúdo de negócio. Campos vazios,
    timestamps inválidos e coordenadas inválidas continuam sendo assunto
    da camada Silver.

    Erros estruturais rejeitam o arquivo inteiro:
    - arquivo vazio;
    - CSV impossível de interpretar;
    - erro de leitura;
    - ausência de colunas necessárias para a Silver.
    """
    if not file_path.is_file():
        return FileValidationResult(
            source_path=file_path,
            is_valid=False,
            error_message="O caminho não representa um arquivo.",
        )

    if file_path.suffix.lower() != ".csv":
        return FileValidationResult(
            source_path=file_path,
            is_valid=False,
            error_message="A extensão do arquivo não é CSV.",
        )

    if file_path.stat().st_size == 0:
        return FileValidationResult(
            source_path=file_path,
            is_valid=False,
            error_message="O arquivo está vazio.",
        )

    try:
        dataframe, detected_encoding = (
            read_csv_with_supported_encoding(file_path)
        )
        dataframe = normalize_column_names(dataframe)

    except EmptyDataError:
        return FileValidationResult(
            source_path=file_path,
            is_valid=False,
            error_message=(
                "O arquivo não contém cabeçalho ou dados legíveis."
            ),
        )

    except ParserError as error:
        return FileValidationResult(
            source_path=file_path,
            is_valid=False,
            error_message=(
                "O CSV possui estrutura inconsistente: "
                f"{error}"
            ),
        )

    except (OSError, UnicodeError, ValueError) as error:
        return FileValidationResult(
            source_path=file_path,
            is_valid=False,
            error_message=(
                "Não foi possível ler o arquivo: "
                f"{error}"
            ),
        )

    missing_columns = validate_dataframe_schema(dataframe)
    reserved_columns = find_reserved_metadata_columns(dataframe)

    if missing_columns:
        return FileValidationResult(
            source_path=file_path,
            is_valid=False,
            row_count=len(dataframe),
            dataframe=None,
            missing_columns=missing_columns,
            error_message=(
                "O arquivo não possui todas as colunas "
                "necessárias para o pipeline."
            ),
            detected_encoding=detected_encoding,
        )

    if reserved_columns:
        return FileValidationResult(
            source_path=file_path,
            is_valid=False,
            row_count=len(dataframe),
            dataframe=None,
            reserved_columns=reserved_columns,
            error_message=(
                "O arquivo utiliza nomes de colunas reservados "
                "para metadados da Bronze."
            ),
            detected_encoding=detected_encoding,
        )

    return FileValidationResult(
        source_path=file_path,
        is_valid=True,
        row_count=len(dataframe),
        dataframe=dataframe,
        detected_encoding=detected_encoding,
    )


def build_available_destination(
    destination_directory: Path,
    file_name: str,
) -> Path:
    """
    Evita sobrescrever um arquivo que já existe no destino.
    """
    destination = destination_directory / file_name

    if not destination.exists():
        return destination

    source_name = Path(file_name)
    timestamp = datetime.now(timezone.utc).strftime(
        "%Y%m%dT%H%M%S%fZ"
    )

    return destination_directory / (
        f"{source_name.stem}__{timestamp}{source_name.suffix}"
    )


def move_file_to_quarantine(
    validation_result: FileValidationResult,
    quarantine_path: Path,
) -> Path:
    """
    Move um arquivo estruturalmente inválido para quarantine e
    cria um relatório de texto ao lado dele.
    """
    source_path = validation_result.source_path
    destination = build_available_destination(
        destination_directory=quarantine_path,
        file_name=source_path.name,
    )

    quarantine_path.mkdir(
        parents=True,
        exist_ok=True,
    )

    shutil.move(
        str(source_path),
        str(destination),
    )

    report_path = destination.with_suffix(
        destination.suffix + ".error.txt"
    )

    missing_columns_text = (
        ", ".join(validation_result.missing_columns)
        if validation_result.missing_columns
        else "Nenhuma"
    )
    reserved_columns_text = (
        ", ".join(validation_result.reserved_columns)
        if validation_result.reserved_columns
        else "Nenhuma"
    )

    report_content = "\n".join(
        (
            f"source_file={source_path.name}",
            f"quarantined_at={datetime.now(timezone.utc).isoformat()}",
            f"reason={validation_result.error_message}",
            f"missing_columns={missing_columns_text}",
            f"reserved_columns={reserved_columns_text}",
            f"detected_encoding={validation_result.detected_encoding}",
            f"row_count={validation_result.row_count}",
        )
    )

    report_path.write_text(
        report_content,
        encoding="utf-8",
    )

    return destination


def calculate_file_hash(
    file_path: Path,
    chunk_size: int = FILE_HASH_CHUNK_SIZE,
) -> str:
    """
    Calcula o SHA-256 do conteúdo binário do arquivo.

    A leitura em blocos evita carregar o arquivo inteiro na memória.
    """
    if chunk_size <= 0:
        raise ValueError("chunk_size precisa ser maior que zero.")

    hasher = sha256()

    with file_path.open("rb") as source_file:
        for chunk in iter(
            lambda: source_file.read(chunk_size),
            b"",
        ):
            hasher.update(chunk)

    return hasher.hexdigest()


def generate_batch_id() -> str:
    """
    Gera o identificador único da execução da ingestão.
    """
    return str(uuid4())


def normalize_ingestion_timestamp(
    ingested_at: datetime | None = None,
) -> datetime:
    """
    Normaliza o instante de ingestão para UTC.
    """
    timestamp = ingested_at or datetime.now(timezone.utc)

    if timestamp.tzinfo is None:
        return timestamp.replace(tzinfo=timezone.utc)

    return timestamp.astimezone(timezone.utc)


def calculate_row_id(
    source_file_hash: str,
    source_row_number: int,
) -> str:
    """
    Gera um identificador determinístico para a linha de origem.

    O batch_id não faz parte do cálculo. Assim, a mesma linha do mesmo
    arquivo mantém o row_id quando a execução é repetida.
    """
    if source_row_number < 1:
        raise ValueError(
            "source_row_number precisa começar em 1."
        )

    identity = (
        f"{source_file_hash}:{source_row_number}"
        .encode("utf-8")
    )

    return sha256(identity).hexdigest()


def prepare_bronze_dataframe(
    validation_result: FileValidationResult,
    batch_id: str,
    ingested_at: datetime | None = None,
) -> pd.DataFrame:
    """
    Acrescenta os metadados técnicos sem alterar as colunas brutas.
    """
    if not validation_result.is_valid:
        raise ValueError(
            "Não é possível preparar um arquivo inválido."
        )

    if validation_result.dataframe is None:
        raise ValueError(
            "Um arquivo validado precisa possuir um DataFrame."
        )

    if not batch_id.strip():
        raise ValueError("batch_id não pode ser vazio.")

    source_path = validation_result.source_path
    source_file_hash = calculate_file_hash(source_path)
    ingestion_timestamp = normalize_ingestion_timestamp(
        ingested_at
    )

    bronze_dataframe = validation_result.dataframe.copy()
    source_row_numbers = list(
        range(1, len(bronze_dataframe) + 1)
    )

    bronze_dataframe["source_file"] = source_path.name
    bronze_dataframe["source_file_hash"] = source_file_hash
    bronze_dataframe["source_row_number"] = source_row_numbers
    bronze_dataframe["row_id"] = [
        calculate_row_id(
            source_file_hash=source_file_hash,
            source_row_number=row_number,
        )
        for row_number in source_row_numbers
    ]
    bronze_dataframe["batch_id"] = batch_id
    bronze_dataframe["ingested_at"] = pd.Timestamp(
        ingestion_timestamp
    )
    bronze_dataframe["ingestion_date"] = (
        ingestion_timestamp.date().isoformat()
    )

    return bronze_dataframe


def write_current_bronze_table(
    project_dir: Path,
    validation_result: FileValidationResult,
    batch_id: str,
    ingested_at: datetime | None = None,
) -> Path:
    """
    Preserva a escrita atual de uma única Delta Table, agora com
    metadados técnicos de ingestão.

    O processamento consolidado dos demais arquivos será implementado
    nas próximas sprints.
    """
    dataframe = prepare_bronze_dataframe(
        validation_result=validation_result,
        batch_id=batch_id,
        ingested_at=ingested_at,
    )

    raw_file_path = validation_result.source_path

    bronze_path = (
        project_dir
        / "data"
        / "lakehouse"
        / "01_bronze"
        / raw_file_path.stem
    )

    bronze_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    print(
        "[Lakehouse][Bronze] "
        f"Gravando Delta Table em: {bronze_path}"
    )

    try:
        # Import local para manter as funções de descoberta e validação
        # testáveis mesmo sem inicializar o mecanismo Delta Lake.
        from deltalake import write_deltalake

        write_deltalake(
            bronze_path,
            dataframe,
            mode="overwrite",
            schema_mode="overwrite",
        )

    except Exception as error:
        raise RuntimeError(
            "O arquivo passou na validação, mas a escrita da "
            f"Bronze falhou: {raw_file_path.name}: {error}"
        ) from error

    print(
        "[Lakehouse][Bronze] "
        f"Delta Table gravada com sucesso: {bronze_path}"
    )
    print(
        "[Lakehouse][Bronze] "
        f"Batch: {batch_id} | linhas={len(dataframe)}"
    )

    return bronze_path


def get_control_table_path(
    project_dir: Path,
) -> Path:
    """
    Retorna o caminho da Delta Table de controle de ingestão.
    """
    return (
        project_dir
        / "data"
        / "lakehouse"
        / "00_control"
        / "ingestion_files"
    )


def is_delta_table(table_path: Path) -> bool:
    """
    Verifica se o caminho já contém o log transacional Delta.
    """
    return (
        table_path.is_dir()
        and (table_path / "_delta_log").is_dir()
    )


def create_control_event(
    *,
    batch_id: str,
    source_file: str,
    source_file_hash: str | None,
    status: str,
    started_at: datetime,
    finished_at: datetime | None = None,
    row_count: int | None = None,
    inserted_row_count: int | None = None,
    duplicate_row_count: int | None = None,
    status_reason: str | None = None,
    error_message: str | None = None,
    recorded_at: datetime | None = None,
) -> IngestionControlEvent:
    """
    Cria e valida um evento da tabela de controle.
    """
    normalized_status = status.strip().upper()

    if normalized_status not in CONTROL_STATUSES:
        raise ValueError(
            "Status de controle inválido: "
            f"{status}. Esperado: {CONTROL_STATUSES}."
        )

    if not batch_id.strip():
        raise ValueError("batch_id não pode ser vazio.")

    if not source_file.strip():
        raise ValueError("source_file não pode ser vazio.")

    for field_name, value in (
        ("row_count", row_count),
        ("inserted_row_count", inserted_row_count),
        ("duplicate_row_count", duplicate_row_count),
    ):
        if value is not None and value < 0:
            raise ValueError(
                f"{field_name} não pode ser negativo."
            )

    normalized_started_at = normalize_ingestion_timestamp(
        started_at
    )
    normalized_recorded_at = normalize_ingestion_timestamp(
        recorded_at
    )
    normalized_finished_at = (
        normalize_ingestion_timestamp(finished_at)
        if finished_at is not None
        else None
    )

    if (
        normalized_finished_at is not None
        and normalized_finished_at < normalized_started_at
    ):
        raise ValueError(
            "finished_at não pode ser anterior a started_at."
        )

    if (
        normalized_status != "PROCESSING"
        and normalized_finished_at is None
    ):
        normalized_finished_at = normalized_recorded_at

    return IngestionControlEvent(
        control_event_id=str(uuid4()),
        batch_id=batch_id,
        source_file=source_file,
        source_file_hash=source_file_hash,
        status=normalized_status,
        stage=CONTROL_STAGE,
        started_at=normalized_started_at,
        finished_at=normalized_finished_at,
        row_count=row_count,
        inserted_row_count=inserted_row_count,
        duplicate_row_count=duplicate_row_count,
        status_reason=status_reason,
        error_message=error_message,
        recorded_at=normalized_recorded_at,
    )


def control_event_to_arrow_table(
    event: IngestionControlEvent,
):
    """
    Converte o evento para uma PyArrow Table com schema explícito.

    O schema explícito evita que campos inicialmente nulos sejam
    persistidos como NullType e causem erro em eventos posteriores.
    """
    import pyarrow as pa

    schema = pa.schema(
        [
            pa.field("control_event_id", pa.string(), nullable=False),
            pa.field("batch_id", pa.string(), nullable=False),
            pa.field("source_file", pa.string(), nullable=False),
            pa.field("source_file_hash", pa.string(), nullable=True),
            pa.field("status", pa.string(), nullable=False),
            pa.field("stage", pa.string(), nullable=False),
            pa.field(
                "started_at",
                pa.timestamp("us", tz="UTC"),
                nullable=False,
            ),
            pa.field(
                "finished_at",
                pa.timestamp("us", tz="UTC"),
                nullable=True,
            ),
            pa.field("row_count", pa.int64(), nullable=True),
            pa.field(
                "inserted_row_count",
                pa.int64(),
                nullable=True,
            ),
            pa.field(
                "duplicate_row_count",
                pa.int64(),
                nullable=True,
            ),
            pa.field("status_reason", pa.string(), nullable=True),
            pa.field("error_message", pa.string(), nullable=True),
            pa.field(
                "recorded_at",
                pa.timestamp("us", tz="UTC"),
                nullable=False,
            ),
        ]
    )

    return pa.Table.from_pylist(
        [
            {
                "control_event_id": event.control_event_id,
                "batch_id": event.batch_id,
                "source_file": event.source_file,
                "source_file_hash": event.source_file_hash,
                "status": event.status,
                "stage": event.stage,
                "started_at": event.started_at,
                "finished_at": event.finished_at,
                "row_count": event.row_count,
                "inserted_row_count": event.inserted_row_count,
                "duplicate_row_count": event.duplicate_row_count,
                "status_reason": event.status_reason,
                "error_message": event.error_message,
                "recorded_at": event.recorded_at,
            }
        ],
        schema=schema,
    )


def append_control_event(
    control_path: Path,
    event: IngestionControlEvent,
) -> None:
    """
    Acrescenta um evento à tabela de controle.
    """
    from deltalake import write_deltalake

    control_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    write_mode = (
        "append"
        if is_delta_table(control_path)
        else "overwrite"
    )

    write_deltalake(
        control_path,
        control_event_to_arrow_table(event),
        mode=write_mode,
    )


def record_ingestion_status(
    control_path: Path,
    *,
    batch_id: str,
    source_file: str,
    source_file_hash: str | None,
    status: str,
    started_at: datetime,
    finished_at: datetime | None = None,
    row_count: int | None = None,
    inserted_row_count: int | None = None,
    duplicate_row_count: int | None = None,
    status_reason: str | None = None,
    error_message: str | None = None,
) -> IngestionControlEvent:
    """
    Cria, persiste e exibe um evento de controle.
    """
    event = create_control_event(
        batch_id=batch_id,
        source_file=source_file,
        source_file_hash=source_file_hash,
        status=status,
        started_at=started_at,
        finished_at=finished_at,
        row_count=row_count,
        inserted_row_count=inserted_row_count,
        duplicate_row_count=duplicate_row_count,
        status_reason=status_reason,
        error_message=error_message,
    )

    append_control_event(
        control_path=control_path,
        event=event,
    )

    print(
        "[Lakehouse][Control] "
        f"status={event.status} "
        f"| arquivo={event.source_file} "
        f"| batch={event.batch_id}"
    )

    return event


def load_successful_file_hashes(
    control_path: Path,
) -> set[str]:
    """
    Carrega os hashes que já tiveram pelo menos um evento SUCCESS.

    PROCESSING, FAILED e SKIPPED não bloqueiam uma nova tentativa.
    """
    if not is_delta_table(control_path):
        return set()

    from deltalake import DeltaTable

    control_dataframe = DeltaTable(
        str(control_path)
    ).to_pandas(
        columns=["source_file_hash", "status"]
    )

    if control_dataframe.empty:
        return set()

    successful_rows = control_dataframe.loc[
        control_dataframe["status"] == "SUCCESS",
        "source_file_hash",
    ].dropna()

    return {
        str(file_hash)
        for file_hash in successful_rows.tolist()
        if str(file_hash).strip()
    }


def should_skip_file_hash(
    source_file_hash: str,
    successful_file_hashes: set[str],
) -> bool:
    """
    Decide se o conteúdo do arquivo já foi concluído com sucesso.
    """
    return source_file_hash in successful_file_hashes


def add_legacy_root_file_if_needed(
    input_files: list[Path],
    raw_path: Path,
) -> list[Path]:
    """
    Mantém compatibilidade temporária com o arquivo em data/raw/.

    Se houver um arquivo de mesmo nome na inbox, a inbox tem prioridade.
    """
    legacy_path = raw_path / LEGACY_SOURCE_FILE

    if not legacy_path.is_file():
        return input_files

    if any(
        file_path.name == LEGACY_SOURCE_FILE
        for file_path in input_files
    ):
        return input_files

    print(
        "[Lakehouse][Bronze][WARNING] "
        f"O arquivo {LEGACY_SOURCE_FILE} ainda está em data/raw/. "
        "Mova-o para data/raw/inbox/."
    )

    return sorted(
        [*input_files, legacy_path],
        key=lambda file_path: file_path.name.lower(),
    )


def process_input_files_with_control(
    *,
    project_dir: Path,
    input_files: list[Path],
    quarantine_path: Path,
    control_path: Path,
    batch_id: str,
    successful_file_hashes: set[str],
) -> list[FileValidationResult]:
    """
    Controla e processa cada arquivo de forma isolada.

    Nesta sprint, apenas o arquivo legado é escrito na Bronze. Outros
    arquivos válidos recebem SKIPPED e permanecem na inbox para a futura
    ativação do processamento múltiplo.
    """
    validation_results: list[FileValidationResult] = []

    for file_path in input_files:
        started_at = datetime.now(timezone.utc)

        try:
            source_file_hash = calculate_file_hash(file_path)
        except Exception as error:
            record_ingestion_status(
                control_path,
                batch_id=batch_id,
                source_file=file_path.name,
                source_file_hash=None,
                status="FAILED",
                started_at=started_at,
                finished_at=datetime.now(timezone.utc),
                error_message=(
                    "Não foi possível calcular o hash do arquivo: "
                    f"{error}"
                ),
            )
            continue

        if should_skip_file_hash(
            source_file_hash,
            successful_file_hashes,
        ):
            record_ingestion_status(
                control_path,
                batch_id=batch_id,
                source_file=file_path.name,
                source_file_hash=source_file_hash,
                status="SKIPPED",
                started_at=started_at,
                finished_at=datetime.now(timezone.utc),
                inserted_row_count=0,
                duplicate_row_count=0,
                status_reason=(
                    "O mesmo conteúdo já possui uma ingestão "
                    "concluída com SUCCESS."
                ),
            )
            continue

        record_ingestion_status(
            control_path,
            batch_id=batch_id,
            source_file=file_path.name,
            source_file_hash=source_file_hash,
            status="PROCESSING",
            started_at=started_at,
        )

        print(
            "[Lakehouse][Bronze][Validation] "
            f"Validando: {file_path.name}"
        )

        validation_result = validate_input_file(file_path)
        validation_results.append(validation_result)

        if not validation_result.is_valid:
            print(
                "[Lakehouse][Bronze][Validation] "
                f"INVALID | arquivo={file_path.name} "
                f"| motivo={validation_result.error_message}"
            )

            record_ingestion_status(
                control_path,
                batch_id=batch_id,
                source_file=file_path.name,
                source_file_hash=source_file_hash,
                status="FAILED",
                started_at=started_at,
                finished_at=datetime.now(timezone.utc),
                row_count=validation_result.row_count,
                inserted_row_count=0,
                duplicate_row_count=0,
                error_message=validation_result.error_message,
            )

            quarantined_file = move_file_to_quarantine(
                validation_result=validation_result,
                quarantine_path=quarantine_path,
            )

            print(
                "[Lakehouse][Bronze][Quarantine] "
                f"Movido para: {quarantined_file}"
            )
            continue

        print(
            "[Lakehouse][Bronze][Validation] "
            f"VALID | arquivo={file_path.name} "
            f"| linhas={validation_result.row_count} "
            f"| encoding={validation_result.detected_encoding}"
        )

        if file_path.name != LEGACY_SOURCE_FILE:
            record_ingestion_status(
                control_path,
                batch_id=batch_id,
                source_file=file_path.name,
                source_file_hash=source_file_hash,
                status="SKIPPED",
                started_at=started_at,
                finished_at=datetime.now(timezone.utc),
                row_count=validation_result.row_count,
                inserted_row_count=0,
                duplicate_row_count=0,
                status_reason=(
                    "Arquivo estruturalmente válido, mas o "
                    "processamento múltiplo ainda não foi ativado."
                ),
            )
            continue

        try:
            write_current_bronze_table(
                project_dir=project_dir,
                validation_result=validation_result,
                batch_id=batch_id,
            )
        except Exception as error:
            record_ingestion_status(
                control_path,
                batch_id=batch_id,
                source_file=file_path.name,
                source_file_hash=source_file_hash,
                status="FAILED",
                started_at=started_at,
                finished_at=datetime.now(timezone.utc),
                row_count=validation_result.row_count,
                inserted_row_count=0,
                duplicate_row_count=0,
                error_message=str(error),
            )
            raise

        record_ingestion_status(
            control_path,
            batch_id=batch_id,
            source_file=file_path.name,
            source_file_hash=source_file_hash,
            status="SUCCESS",
            started_at=started_at,
            finished_at=datetime.now(timezone.utc),
            row_count=validation_result.row_count,
            inserted_row_count=validation_result.row_count,
            duplicate_row_count=0,
        )

        successful_file_hashes.add(source_file_hash)

    return validation_results


def print_validation_summary(
    validation_results: list[FileValidationResult],
) -> None:
    """
    Exibe o resumo da validação da execução.
    """
    valid_count = sum(
        result.is_valid
        for result in validation_results
    )
    invalid_count = len(validation_results) - valid_count

    print(
        "[Lakehouse][Bronze][Validation] "
        f"Resumo | válidos={valid_count} "
        f"| inválidos={invalid_count} "
        f"| total={len(validation_results)}"
    )


def load_bronze_data() -> list[FileValidationResult]:
    """
    Executa a Sprint 4 da Bronze.

    Responsabilidades:
    1. descobrir e validar arquivos individualmente;
    2. manter os metadados de linhagem da Sprint 3;
    3. registrar PROCESSING, SUCCESS, FAILED e SKIPPED;
    4. não reprocessar hashes já concluídos com SUCCESS;
    5. preservar temporariamente a escrita do arquivo legado.
    """
    project_dir = Path(__file__).resolve().parent.parent
    batch_id = generate_batch_id()
    control_path = get_control_table_path(project_dir)

    print(
        "[Lakehouse][Bronze] "
        f"Iniciando batch: {batch_id}"
    )

    (
        raw_path,
        inbox_path,
        archive_path,
        quarantine_path,
    ) = create_raw_directories(project_dir)

    print(
        "[Lakehouse][Bronze] "
        f"Inbox: {inbox_path}"
    )
    print(
        "[Lakehouse][Bronze] "
        f"Archive preparado: {archive_path}"
    )
    print(
        "[Lakehouse][Bronze] "
        f"Quarantine: {quarantine_path}"
    )
    print(
        "[Lakehouse][Control] "
        f"Tabela: {control_path}"
    )

    input_files = discover_input_files(inbox_path)
    input_files = add_legacy_root_file_if_needed(
        input_files=input_files,
        raw_path=raw_path,
    )
    print_discovered_files(input_files)

    if not input_files:
        print(
            "[Lakehouse][Bronze] "
            "Nada para processar nesta execução."
        )
        return []

    successful_file_hashes = load_successful_file_hashes(
        control_path
    )

    print(
        "[Lakehouse][Control] "
        "Hashes concluídos anteriormente: "
        f"{len(successful_file_hashes)}"
    )

    validation_results = process_input_files_with_control(
        project_dir=project_dir,
        input_files=input_files,
        quarantine_path=quarantine_path,
        control_path=control_path,
        batch_id=batch_id,
        successful_file_hashes=successful_file_hashes,
    )

    print_validation_summary(validation_results)

    return validation_results


if __name__ == "__main__":
    load_bronze_data()