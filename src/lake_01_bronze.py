from pathlib import Path

import pandas as pd
from deltalake import write_deltalake


LEGACY_SOURCE_FILE = "logs_rastreador_2026-07-01.csv"


def create_raw_directories(
    project_dir: Path,
) -> tuple[Path, Path, Path, Path]:
    """
    Cria os diretórios usados pela futura ingestão de múltiplos arquivos.

    Nesta sprint, archive e quarantine são apenas preparados.
    A movimentação dos arquivos será implementada nas próximas sprints.
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

    A ordenação por nome torna o resultado determinístico.
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
    Exibe os arquivos encontrados de forma legível.
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


def resolve_current_source_file(
    raw_path: Path,
    inbox_path: Path,
) -> Path | None:
    """
    Mantém temporariamente a compatibilidade com o pipeline atual.

    A Silver ainda espera a Bronze correspondente ao arquivo
    logs_rastreador_2026-07-01.csv. Por isso, nesta sprint,
    somente esse arquivo continua sendo escrito na Bronze.

    Primeiro procura o arquivo na nova inbox. Durante a transição,
    também aceita o local antigo: data/raw/.
    """
    inbox_source = inbox_path / LEGACY_SOURCE_FILE

    if inbox_source.is_file():
        return inbox_source

    legacy_source = raw_path / LEGACY_SOURCE_FILE

    if legacy_source.is_file():
        print(
            "[Lakehouse][Bronze][WARNING] "
            f"O arquivo {LEGACY_SOURCE_FILE} ainda está em "
            "data/raw/. Mova-o para data/raw/inbox/."
        )
        return legacy_source

    return None


def write_current_bronze_table(
    project_dir: Path,
    raw_file_path: Path,
) -> None:
    """
    Preserva a escrita atual de uma única Delta Table.

    O processamento real de todos os arquivos encontrados será
    implementado nas próximas sprints.
    """
    bronze_path = (
        project_dir
        / "data"
        / "lakehouse"
        / "01_bronze"
        / raw_file_path.stem
    )

    print(
        "[Lakehouse][Bronze] "
        f"Carregando arquivo atual: {raw_file_path}"
    )

    try:
        dataframe = pd.read_csv(raw_file_path)

        bronze_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        print(
            "[Lakehouse][Bronze] "
            f"Gravando Delta Table em: {bronze_path}"
        )

        write_deltalake(
            bronze_path,
            dataframe,
            mode="overwrite",
        )

        print(
            "[Lakehouse][Bronze] "
            f"Delta Table gravada com sucesso: {bronze_path}"
        )

    except Exception as error:
        raise RuntimeError(
            "Ocorreu um erro ao carregar o arquivo atual "
            f"{raw_file_path.name}: {error}"
        ) from error


def load_bronze_data() -> list[Path]:
    """
    Executa a Sprint 1 da Bronze.

    Responsabilidades desta sprint:
    1. criar inbox, archive e quarantine;
    2. descobrir todos os CSVs da inbox;
    3. listar os arquivos encontrados;
    4. preservar a escrita do arquivo já usado pela Silver atual.

    Retorna a lista de arquivos encontrados para permitir testes
    e futura integração com o pipeline.
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
        f"Quarantine preparada: {quarantine_path}"
    )

    input_files = discover_input_files(inbox_path)
    print_discovered_files(input_files)

    current_source_file = resolve_current_source_file(
        raw_path=raw_path,
        inbox_path=inbox_path,
    )

    if current_source_file is None:
        if input_files:
            print(
                "[Lakehouse][Bronze][WARNING] "
                "Os arquivos foram descobertos, mas ainda não serão "
                "gravados nesta sprint. O arquivo esperado pelo "
                f"pipeline atual é {LEGACY_SOURCE_FILE}."
            )
        else:
            print(
                "[Lakehouse][Bronze] "
                "Nada para processar nesta execução."
            )

        return input_files

    write_current_bronze_table(
        project_dir=project_dir,
        raw_file_path=current_source_file,
    )

    return input_files


if __name__ == "__main__":
    load_bronze_data()