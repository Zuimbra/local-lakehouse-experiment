from __future__ import annotations

import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from pandas.errors import EmptyDataError, ParserError


LEGACY_SOURCE_FILE = "logs_rastreador_2026-07-01.csv"

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
    error_message: str | None = None
    detected_encoding: str | None = None


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

    Colunas extras são aceitas.
    """
    available_columns = set(dataframe.columns)

    return tuple(
        column
        for column in EXPECTED_COLUMNS
        if column not in available_columns
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

    report_content = "\n".join(
        (
            f"source_file={source_path.name}",
            f"quarantined_at={datetime.now(timezone.utc).isoformat()}",
            f"reason={validation_result.error_message}",
            f"missing_columns={missing_columns_text}",
            f"detected_encoding={validation_result.detected_encoding}",
            f"row_count={validation_result.row_count}",
        )
    )

    report_path.write_text(
        report_content,
        encoding="utf-8",
    )

    return destination


def validate_discovered_files(
    input_files: list[Path],
    quarantine_path: Path,
) -> list[FileValidationResult]:
    """
    Valida cada arquivo isoladamente.

    Uma falha não interrompe a validação dos outros arquivos.
    """
    results: list[FileValidationResult] = []

    for file_path in input_files:
        print(
            "[Lakehouse][Bronze][Validation] "
            f"Validando: {file_path.name}"
        )

        result = validate_input_file(file_path)
        results.append(result)

        if result.is_valid:
            print(
                "[Lakehouse][Bronze][Validation] "
                f"VALID | arquivo={file_path.name} "
                f"| linhas={result.row_count} "
                f"| encoding={result.detected_encoding}"
            )
            continue

        print(
            "[Lakehouse][Bronze][Validation] "
            f"INVALID | arquivo={file_path.name} "
            f"| motivo={result.error_message}"
        )

        if result.missing_columns:
            print(
                "[Lakehouse][Bronze][Validation] "
                "Colunas ausentes: "
                + ", ".join(result.missing_columns)
            )

        quarantined_file = move_file_to_quarantine(
            validation_result=result,
            quarantine_path=quarantine_path,
        )

        print(
            "[Lakehouse][Bronze][Quarantine] "
            f"Movido para: {quarantined_file}"
        )

    return results


def find_valid_legacy_source(
    validation_results: list[FileValidationResult],
) -> FileValidationResult | None:
    """
    Localiza o arquivo que continua alimentando a Bronze atual.
    """
    for result in validation_results:
        if (
            result.is_valid
            and result.source_path.name == LEGACY_SOURCE_FILE
        ):
            return result

    return None


def validate_legacy_root_file(
    raw_path: Path,
    quarantine_path: Path,
) -> FileValidationResult | None:
    """
    Compatibilidade temporária com o CSV no antigo data/raw/.

    Novos arquivos devem ser colocados em data/raw/inbox/.
    """
    legacy_path = raw_path / LEGACY_SOURCE_FILE

    if not legacy_path.is_file():
        return None

    print(
        "[Lakehouse][Bronze][WARNING] "
        f"O arquivo {LEGACY_SOURCE_FILE} ainda está em data/raw/. "
        "Mova-o para data/raw/inbox/."
    )

    result = validate_input_file(legacy_path)

    if not result.is_valid:
        quarantined_file = move_file_to_quarantine(
            validation_result=result,
            quarantine_path=quarantine_path,
        )
        print(
            "[Lakehouse][Bronze][Quarantine] "
            f"Arquivo antigo inválido movido para: {quarantined_file}"
        )

    return result


def write_current_bronze_table(
    project_dir: Path,
    validation_result: FileValidationResult,
) -> None:
    """
    Preserva a escrita atual de uma única Delta Table.

    O processamento consolidado dos demais arquivos será implementado
    nas próximas sprints.
    """
    dataframe = validation_result.dataframe

    if dataframe is None:
        raise ValueError(
            "Um arquivo validado precisa possuir um DataFrame."
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
    Executa a Sprint 2 da Bronze.

    Responsabilidades:
    1. criar inbox, archive e quarantine;
    2. descobrir todos os CSVs da inbox;
    3. validar cada arquivo isoladamente;
    4. mover arquivos estruturalmente inválidos para quarantine;
    5. preservar a escrita do arquivo esperado pela Silver atual.

    Arquivos válidos diferentes do arquivo legado permanecem na inbox
    aguardando a Sprint de processamento múltiplo.
    """
    project_dir = Path(__file__).resolve().parent.parent

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

    input_files = discover_input_files(inbox_path)
    print_discovered_files(input_files)

    validation_results = validate_discovered_files(
        input_files=input_files,
        quarantine_path=quarantine_path,
    )

    legacy_result = find_valid_legacy_source(
        validation_results
    )

    if legacy_result is None:
        root_legacy_result = validate_legacy_root_file(
            raw_path=raw_path,
            quarantine_path=quarantine_path,
        )

        if (
            root_legacy_result is not None
            and root_legacy_result.is_valid
        ):
            legacy_result = root_legacy_result
            validation_results.append(root_legacy_result)

    print_validation_summary(validation_results)

    if legacy_result is None:
        valid_files = [
            result.source_path.name
            for result in validation_results
            if result.is_valid
        ]

        if valid_files:
            print(
                "[Lakehouse][Bronze][WARNING] "
                "Existem arquivos válidos, mas eles ainda não serão "
                "gravados nesta sprint. A Silver atual espera "
                f"{LEGACY_SOURCE_FILE}."
            )
        else:
            print(
                "[Lakehouse][Bronze] "
                "Nenhum arquivo válido disponível para escrita."
            )

        return validation_results

    write_current_bronze_table(
        project_dir=project_dir,
        validation_result=legacy_result,
    )

    return validation_results


if __name__ == "__main__":
    load_bronze_data()