from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
from deltalake import DeltaTable, write_deltalake


BRONZE_TABLE_NAME = "tracker_logs"

BRONZE_METADATA_COLUMNS = (
    "source_file",
    "source_file_hash",
    "source_row_number",
    "row_id",
    "batch_id",
    "ingested_at",
    "ingestion_date",
)


@dataclass(frozen=True)
class SilverLoadResult:
    """
    Resume o processamento executado pela Silver.

    A Sprint 8 utilizará affected_event_dates para limitar também
    o reprocessamento da Gold.
    """

    mode: str
    batch_ids: tuple[str, ...]
    affected_event_dates: tuple[str, ...]
    affected_rejection_dates: tuple[str, ...]
    telemetry_rows_written: int
    identity_rows_written: int
    rejected_rows_written: int


def is_delta_table(path: Path) -> bool:
    """
    Retorna True quando o caminho representa uma Delta Table local.
    """
    return (
        path.is_dir()
        and (path / "_delta_log").is_dir()
    )


def get_lakehouse_paths(
    project_dir: Path,
) -> dict[str, Path]:
    """
    Centraliza os caminhos usados pela camada Silver.
    """
    bronze_path = (
        project_dir
        / "data"
        / "lakehouse"
        / "01_bronze"
        / BRONZE_TABLE_NAME
    )

    silver_path = (
        project_dir
        / "data"
        / "lakehouse"
        / "02_silver"
    )

    return {
        "bronze": bronze_path,
        "silver": silver_path,
        "telemetry": silver_path / "telemetry_events",
        "identity": silver_path / "device_identity_events",
        "rejected": silver_path / "rejected_logs",
    }


def load_bronze_table(
    bronze_path: Path,
) -> DeltaTable:
    """
    Valida e carrega a Bronze consolidada.
    """
    if not bronze_path.is_dir():
        raise FileNotFoundError(
            "The Bronze Delta Table does not exist: "
            f"{bronze_path}"
        )

    if not is_delta_table(bronze_path):
        raise ValueError(
            "The Bronze path is not a valid Delta Table: "
            f"{bronze_path}"
        )

    try:
        return DeltaTable(str(bronze_path))

    except Exception as error:
        raise RuntimeError(
            "Could not load the Bronze Delta Table: "
            f"{bronze_path}. Reason: {error}"
        ) from error


def get_delta_column_names(
    delta_table: DeltaTable,
) -> set[str]:
    """
    Obtém os nomes das colunas sem depender de conversões de Schema
    que mudaram entre versões do delta-rs.
    """
    return {
        field.name
        for field in delta_table.schema().fields
    }


def validate_bronze_metadata(
    bronze_table: DeltaTable,
) -> None:
    """
    Confirma que a Bronze é compatível com a Sprint 5.

    A Silver não deve voltar silenciosamente a fabricar source_file
    ou perder informações de linhagem.
    """
    available_columns = get_delta_column_names(
        bronze_table
    )

    missing_columns = [
        column
        for column in BRONZE_METADATA_COLUMNS
        if column not in available_columns
    ]

    if missing_columns:
        raise ValueError(
            "The Bronze Delta Table is missing ingestion metadata: "
            + ", ".join(missing_columns)
        )


def get_delta_field_type(
    table_path: Path,
    column_name: str,
) -> str | None:
    """
    Retorna o tipo primitivo Delta de uma coluna.

    PrimitiveType.type é a API pública do delta-rs para obter valores
    como "string", "date", "timestamp" etc.
    """
    if not is_delta_table(table_path):
        return None

    table = DeltaTable(str(table_path))

    for field in table.schema().fields:
        if field.name != column_name:
            continue

        field_type = field.type
        primitive_type = getattr(
            field_type,
            "type",
            None,
        )

        if primitive_type is None:
            return str(field_type).lower()

        return str(primitive_type).lower()

    return None


def silver_supports_incremental_update(
    paths: dict[str, Path],
) -> bool:
    """
    Confirma que as tabelas Silver já estão no schema da Sprint 7.

    event_date e rejection_date passam a ser strings YYYY-MM-DD.
    Se o usuário vier diretamente da Sprint 6, a primeira execução
    desta sprint fará um rebuild completo para migrar o schema.
    """
    expected = (
        ("telemetry", "event_date"),
        ("identity", "event_date"),
        ("rejected", "rejection_date"),
    )

    for path_key, partition_column in expected:
        table_path = paths[path_key]

        if not is_delta_table(table_path):
            return False

        table = DeltaTable(str(table_path))

        if table.metadata().partition_columns != [
            partition_column
        ]:
            return False

        field_type = get_delta_field_type(
            table_path,
            partition_column,
        )

        if field_type != "string":
            return False

    return True


def normalize_batch_ids(
    batch_ids: set[str] | list[str] | tuple[str, ...] | None,
) -> tuple[str, ...]:
    """
    Limpa e ordena os batch_ids recebidos.
    """
    if batch_ids is None:
        return ()

    return tuple(
        sorted(
            {
                str(batch_id).strip()
                for batch_id in batch_ids
                if str(batch_id).strip()
            }
        )
    )


def discover_affected_partitions(
    con: duckdb.DuckDBPyConnection,
    batch_ids: tuple[str, ...],
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """
    Descobre as partições Silver impactadas pelos batches informados.

    Para timestamps válidos, a mesma data pode afetar telemetria,
    identidade ou rejeições. Registros sem timestamp válido afetam a
    partição especial rejected_logs/rejection_date=unknown.
    """
    if not batch_ids:
        return (), ()

    requested_batches = pd.DataFrame(
        {"batch_id": list(batch_ids)}
    )
    con.register(
        "requested_silver_batches",
        requested_batches,
    )

    affected_dates_df = con.execute(
        """
        WITH requested_rows AS (
            SELECT
                COALESCE(
                    TRY_CAST(
                        bronze."TM_STAMP" AS TIMESTAMP
                    ),
                    TRY_CAST(
                        bronze."DATA_SERVIDOR" AS TIMESTAMP
                    )
                ) AS event_timestamp

            FROM bronze

            INNER JOIN requested_silver_batches
                ON CAST(bronze.batch_id AS VARCHAR)
                 = requested_silver_batches.batch_id
        )

        SELECT DISTINCT
            STRFTIME(
                event_timestamp,
                '%Y-%m-%d'
            ) AS event_date

        FROM requested_rows

        WHERE event_timestamp IS NOT NULL

        ORDER BY event_date
        """
    ).df()

    has_unknown = bool(
        con.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM bronze
                INNER JOIN requested_silver_batches
                    ON CAST(
                        bronze.batch_id AS VARCHAR
                    ) = requested_silver_batches.batch_id
                WHERE COALESCE(
                    TRY_CAST(
                        bronze."TM_STAMP" AS TIMESTAMP
                    ),
                    TRY_CAST(
                        bronze."DATA_SERVIDOR" AS TIMESTAMP
                    )
                ) IS NULL
            )
            """
        ).fetchone()[0]
    )

    event_dates = tuple(
        affected_dates_df["event_date"]
        .dropna()
        .astype(str)
        .tolist()
    )

    rejection_dates = (
        (*event_dates, "unknown")
        if has_unknown
        else event_dates
    )

    return (
        event_dates,
        tuple(dict.fromkeys(rejection_dates)),
    )


def create_bronze_scope_view(
    con: duckdb.DuckDBPyConnection,
    *,
    full_rebuild: bool,
    affected_event_dates: tuple[str, ...],
    include_unknown_timestamp: bool,
) -> None:
    """
    Cria a fonte de trabalho da Silver.

    No modo incremental, NÃO filtramos somente os novos batches.
    Filtramos todas as linhas Bronze das datas afetadas. Isso é o que
    permite reconstruir uma partição completa de forma idempotente.
    """
    if full_rebuild:
        con.execute(
            """
            CREATE OR REPLACE TEMP VIEW bronze_scope AS
            SELECT *
            FROM bronze
            """
        )
        return

    affected_dates = pd.DataFrame(
        {"event_date": list(affected_event_dates)}
    )
    con.register(
        "affected_silver_dates",
        affected_dates,
    )

    unknown_sql = (
        "TRUE"
        if include_unknown_timestamp
        else "FALSE"
    )

    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW bronze_scope AS

        WITH scoped AS (
            SELECT
                bronze.*,

                COALESCE(
                    TRY_CAST(
                        bronze."TM_STAMP" AS TIMESTAMP
                    ),
                    TRY_CAST(
                        bronze."DATA_SERVIDOR" AS TIMESTAMP
                    )
                ) AS _scope_event_timestamp

            FROM bronze
        )

        SELECT
            scoped.* EXCLUDE (_scope_event_timestamp)

        FROM scoped

        LEFT JOIN affected_silver_dates
            ON STRFTIME(
                scoped._scope_event_timestamp,
                '%Y-%m-%d'
            ) = affected_silver_dates.event_date

        WHERE
            affected_silver_dates.event_date IS NOT NULL

            OR (
                {unknown_sql}
                AND scoped._scope_event_timestamp IS NULL
            )
        """
    )


def get_arrow_column_names(
    table: pa.Table,
) -> set[str]:
    """
    Retorna os nomes de colunas de uma tabela Arrow.
    """
    return set(table.column_names)


def get_arrow_unique_strings(
    table: pa.Table,
    column_name: str,
) -> tuple[str, ...]:
    """
    Retorna valores únicos não nulos de uma coluna Arrow como strings.
    """
    if column_name not in table.column_names:
        return ()

    values = {
        str(value)
        for value in table[column_name].to_pylist()
        if value is not None
    }

    return tuple(sorted(values))


def validate_arrow_partition_column(
    table: pa.Table,
    partition_by: str,
) -> None:
    """
    Confirma que a coluna de partição existe e é textual.

    STRFTIME no DuckDB produz VARCHAR, então esse tipo deve permanecer
    string mesmo quando a consulta retorna zero linhas.
    """
    if partition_by not in table.column_names:
        raise ValueError(
            "Partition column is missing from Silver table: "
            f"{partition_by}"
        )

    field = table.schema.field(partition_by)

    if not (
        pa.types.is_string(field.type)
        or pa.types.is_large_string(field.type)
    ):
        raise ValueError(
            "Silver partition column must be string: "
            f"{partition_by}={field.type}"
        )


def write_full_silver_table(
    path: Path,
    table: pa.Table,
    *,
    partition_by: str,
) -> int:
    """
    Rebuild completo preservando o schema tipado vindo do DuckDB.

    Usar Arrow diretamente evita que resultados vazios ou colunas
    totalmente NULL sejam inferidos como tipo Null pelo Pandas/Delta.
    """
    validate_arrow_partition_column(
        table,
        partition_by,
    )

    write_deltalake(
        path,
        table,
        mode="overwrite",
        schema_mode="overwrite",
        partition_by=[partition_by],
    )

    return table.num_rows


def escape_delta_string_literal(value: str) -> str:
    """
    Escapa aspas simples para uso em predicados SQL do Delta.
    """
    return value.replace("'", "''")


def filter_arrow_partition(
    table: pa.Table,
    *,
    partition_by: str,
    partition_value: str,
) -> pa.Table:
    """
    Filtra uma única partição sem converter os dados para Pandas.
    """
    column = table[partition_by]
    mask = pc.equal(
        column,
        pa.scalar(
            partition_value,
            type=column.type,
        ),
    )

    return table.filter(mask)


def write_incremental_silver_partitions(
    path: Path,
    table: pa.Table,
    *,
    partition_by: str,
) -> int:
    """
    Substitui somente as partições presentes na tabela Arrow.

    O schema permanece exatamente o definido pelo DuckDB, inclusive
    quando algumas colunas possuem somente NULL na partição atual.
    """
    validate_arrow_partition_column(
        table,
        partition_by,
    )

    if table.num_rows == 0:
        return 0

    if not is_delta_table(path):
        raise RuntimeError(
            "Incremental Silver requires an existing Delta Table: "
            f"{path}"
        )

    partition_values = get_arrow_unique_strings(
        table,
        partition_by,
    )

    rows_written = 0

    for partition_value in partition_values:
        partition_table = filter_arrow_partition(
            table,
            partition_by=partition_by,
            partition_value=partition_value,
        )

        escaped_value = escape_delta_string_literal(
            partition_value
        )

        predicate = (
            f"{partition_by} = '{escaped_value}'"
        )

        print(
            "[Lakehouse][Silver][Incremental] "
            f"Replacing {partition_by}={partition_value} "
            f"| rows={partition_table.num_rows}"
        )

        write_deltalake(
            path,
            partition_table,
            mode="overwrite",
            predicate=predicate,
        )

        rows_written += partition_table.num_rows

    return rows_written


def write_silver_table(
    path: Path,
    table: pa.Table,
    *,
    partition_by: str,
    full_rebuild: bool,
) -> int:
    """
    Escolhe rebuild completo ou replaceWhere por partição.
    """
    if full_rebuild:
        return write_full_silver_table(
            path,
            table,
            partition_by=partition_by,
        )

    return write_incremental_silver_partitions(
        path,
        table,
        partition_by=partition_by,
    )


def load_silver_data(
    project_dir: Path | None = None,
    batch_ids: set[str] | list[str] | tuple[str, ...] | None = None,
) -> SilverLoadResult:
    """
    Atualiza a Silver a partir da Bronze consolidada.

    - Sem batch_ids: rebuild completo, preservando compatibilidade
      com o pipeline atual.
    - Com batch_ids: descobre as datas afetadas e substitui somente
      essas partições.

    A Sprint 8 fará a Bronze retornar os batch_ids novos e conectará
    automaticamente as camadas.
    """
    if project_dir is None:
        project_dir = (
            Path(__file__).resolve().parent.parent
        )

    paths = get_lakehouse_paths(project_dir)

    bronze_path = paths["bronze"]
    silver_path = paths["silver"]
    telemetry_path = paths["telemetry"]
    identity_path = paths["identity"]
    rejected_path = paths["rejected"]

    silver_path.mkdir(
        parents=True,
        exist_ok=True,
    )

    bronze_table = load_bronze_table(bronze_path)
    validate_bronze_metadata(bronze_table)

    normalized_batch_ids = normalize_batch_ids(batch_ids)

    requested_incremental = bool(
        normalized_batch_ids
    )
    incremental_supported = (
        silver_supports_incremental_update(paths)
    )

    full_rebuild = (
        not requested_incremental
        or not incremental_supported
    )

    if requested_incremental and not incremental_supported:
        print(
            "[Lakehouse][Silver][Migration] "
            "Existing Silver tables are not yet compatible with "
            "Sprint 7 incremental partitions. Running one full "
            "rebuild to migrate the schema."
        )

    print(
        "[Lakehouse][Silver] "
        f"Reading consolidated Bronze: {bronze_path}"
    )

    con = duckdb.connect()

    try:
        con.register(
            "bronze",
            bronze_table.to_pyarrow_dataset(),
        )

        if full_rebuild:
            affected_event_dates: tuple[str, ...] = ()
            affected_rejection_dates: tuple[str, ...] = ()
        else:
            (
                affected_event_dates,
                affected_rejection_dates,
            ) = discover_affected_partitions(
                con,
                normalized_batch_ids,
            )

            if (
                not affected_event_dates
                and not affected_rejection_dates
            ):
                print(
                    "[Lakehouse][Silver][Incremental] "
                    "No Bronze rows found for the requested batches."
                )

                return SilverLoadResult(
                    mode="NOOP",
                    batch_ids=normalized_batch_ids,
                    affected_event_dates=(),
                    affected_rejection_dates=(),
                    telemetry_rows_written=0,
                    identity_rows_written=0,
                    rejected_rows_written=0,
                )

        create_bronze_scope_view(
            con,
            full_rebuild=full_rebuild,
            affected_event_dates=affected_event_dates,
            include_unknown_timestamp=(
                "unknown" in affected_rejection_dates
            ),
        )

        # =========================================================
        # VIEW COMUM DA BRONZE
        # =========================================================
        #
        # Normalizações compartilhadas:
        # - renomeia colunas do protocolo;
        # - trim;
        # - strings vazias -> NULL;
        # - TRY_CAST de timestamps;
        # - campos do protocolo permanecem inicialmente como texto;
        # - metadados da Bronze são preservados sem fabricação.
        #
        # BAT_VOLT, LAT e LONT não são convertidos aqui porque as
        # mensagens T1 reutilizam essas posições para identificadores.
        # =========================================================
        con.execute(
            """
            CREATE OR REPLACE TEMP VIEW bronze_normalized AS

            SELECT
                TRY_CAST(
                    "DATA_SERVIDOR" AS TIMESTAMP
                ) AS server_timestamp,

                TRY_CAST(
                    "TM_STAMP" AS TIMESTAMP
                ) AS device_timestamp,

                NULLIF(
                    TRIM(CAST("TIPO_LOG" AS VARCHAR)),
                    ''
                ) AS log_type,

                NULLIF(
                    TRIM(CAST("MESS_TYPE" AS VARCHAR)),
                    ''
                ) AS message_type,

                NULLIF(
                    TRIM(CAST("REPT_TYPE" AS VARCHAR)),
                    ''
                ) AS report_type_raw,

                NULLIF(
                    TRIM(CAST("PRT_VER" AS VARCHAR)),
                    ''
                ) AS protocol_version,

                NULLIF(
                    TRIM(CAST("S/N ou IMEI" AS VARCHAR)),
                    ''
                ) AS device_serial_raw,

                NULLIF(
                    TRIM(CAST("TERM_STATUS" AS VARCHAR)),
                    ''
                ) AS terminal_status,

                NULLIF(
                    TRIM(CAST("BAT_VOLT" AS VARCHAR)),
                    ''
                ) AS battery_voltage_raw,

                NULLIF(
                    TRIM(CAST("LOC_STATUS" AS VARCHAR)),
                    ''
                ) AS location_status_raw,

                NULLIF(
                    TRIM(CAST("LAT" AS VARCHAR)),
                    ''
                ) AS latitude_raw,

                NULLIF(
                    TRIM(CAST("LONT" AS VARCHAR)),
                    ''
                ) AS longitude_raw,

                NULLIF(
                    TRIM(CAST("SPEED" AS VARCHAR)),
                    ''
                ) AS speed_raw,

                NULLIF(
                    TRIM(CAST("DIR" AS VARCHAR)),
                    ''
                ) AS direction_raw,

                NULLIF(
                    TRIM(CAST("INT_BATT" AS VARCHAR)),
                    ''
                ) AS internal_battery_raw,

                NULLIF(
                    TRIM(CAST("ODO_TRIP" AS VARCHAR)),
                    ''
                ) AS odometer_trip_raw,

                NULLIF(
                    TRIM(CAST("ODO_TOTAL" AS VARCHAR)),
                    ''
                ) AS odometer_total_raw,

                NULLIF(
                    TRIM(CAST("HORIMETER" AS VARCHAR)),
                    ''
                ) AS horimeter_raw,

                NULLIF(
                    TRIM(CAST("HDOP" AS VARCHAR)),
                    ''
                ) AS hdop_raw,

                NULLIF(
                    TRIM(CAST("MCC" AS VARCHAR)),
                    ''
                ) AS mcc,

                NULLIF(
                    TRIM(CAST("MNC" AS VARCHAR)),
                    ''
                ) AS mnc,

                NULLIF(
                    TRIM(CAST("LAC" AS VARCHAR)),
                    ''
                ) AS lac,

                NULLIF(
                    TRIM(CAST("CELL_ID" AS VARCHAR)),
                    ''
                ) AS cell_id,

                NULLIF(
                    TRIM(CAST("RX_LEVEL" AS VARCHAR)),
                    ''
                ) AS rx_level_raw,

                NULLIF(
                    TRIM(CAST("SER_COUNT" AS VARCHAR)),
                    ''
                ) AS serial_count_raw,

                NULLIF(
                    TRIM(CAST("TX_TECH" AS VARCHAR)),
                    ''
                ) AS transmission_technology,

                NULLIF(
                    TRIM(CAST("GRP_MSG" AS VARCHAR)),
                    ''
                ) AS message_group,

                NULLIF(
                    TRIM(CAST("IO_STATUS" AS VARCHAR)),
                    ''
                ) AS io_status,

                NULLIF(
                    TRIM(CAST("DRIVER_ID" AS VARCHAR)),
                    ''
                ) AS driver_id,

                NULLIF(
                    TRIM(CAST("PASS_ID" AS VARCHAR)),
                    ''
                ) AS passenger_id,

                NULLIF(
                    TRIM(CAST("RPM" AS VARCHAR)),
                    ''
                ) AS rpm_raw,

                NULLIF(
                    TRIM(CAST("TACHO_SPD" AS VARCHAR)),
                    ''
                ) AS tachograph_speed_raw,

                NULLIF(
                    TRIM(CAST("TACHO_ODO" AS VARCHAR)),
                    ''
                ) AS tachograph_odometer_raw,

                NULLIF(
                    TRIM(CAST("TEMP_1" AS VARCHAR)),
                    ''
                ) AS temperature_1_raw,

                NULLIF(
                    TRIM(CAST("TEMP_2" AS VARCHAR)),
                    ''
                ) AS temperature_2_raw,

                NULLIF(
                    TRIM(CAST("TEMP_3" AS VARCHAR)),
                    ''
                ) AS temperature_3_raw,

                NULLIF(
                    TRIM(CAST("TEMP_4" AS VARCHAR)),
                    ''
                ) AS temperature_4_raw,

                CAST(source_file AS VARCHAR)
                    AS source_file,

                CAST(source_file_hash AS VARCHAR)
                    AS source_file_hash,

                CAST(source_row_number AS BIGINT)
                    AS source_row_number,

                CAST(row_id AS VARCHAR)
                    AS row_id,

                CAST(batch_id AS VARCHAR)
                    AS batch_id,

                CAST(ingested_at AS TIMESTAMP)
                    AS ingested_at,

                CAST(ingestion_date AS DATE)
                    AS ingestion_date

            FROM bronze_scope
            """
        )

        # =========================================================
        # SILVER: TELEMETRY EVENTS
        # =========================================================
        print(
            "[Lakehouse][Silver] "
            "Creating telemetry_events..."
        )

        telemetry_table = con.execute(
            """
            WITH typed_telemetry AS (
                SELECT
                    server_timestamp,
                    device_timestamp,

                    COALESCE(
                        device_timestamp,
                        server_timestamp
                    ) AS event_timestamp,

                    log_type,
                    message_type,

                    TRY_CAST(
                        TRY_CAST(
                            report_type_raw AS DOUBLE
                        ) AS INTEGER
                    ) AS report_type,

                    protocol_version,

                    REGEXP_REPLACE(
                        device_serial_raw,
                        '^M',
                        ''
                    ) AS device_serial,

                    terminal_status,

                    TRY_CAST(
                        battery_voltage_raw AS DOUBLE
                    ) AS battery_voltage,

                    location_status_raw
                        AS location_status,

                    TRY_CAST(
                        latitude_raw AS DOUBLE
                    ) AS latitude,

                    TRY_CAST(
                        longitude_raw AS DOUBLE
                    ) AS longitude,

                    TRY_CAST(
                        speed_raw AS DOUBLE
                    ) AS speed,

                    TRY_CAST(
                        direction_raw AS DOUBLE
                    ) AS direction_degrees,

                    TRY_CAST(
                        internal_battery_raw AS DOUBLE
                    ) AS internal_battery,

                    TRY_CAST(
                        odometer_trip_raw AS DOUBLE
                    ) AS odometer_trip,

                    TRY_CAST(
                        odometer_total_raw AS DOUBLE
                    ) AS odometer_total,

                    TRY_CAST(
                        horimeter_raw AS DOUBLE
                    ) AS horimeter,

                    TRY_CAST(
                        hdop_raw AS DOUBLE
                    ) AS hdop,

                    mcc,
                    mnc,
                    lac,
                    cell_id,

                    TRY_CAST(
                        rx_level_raw AS DOUBLE
                    ) AS rx_level,

                    TRY_CAST(
                        TRY_CAST(
                            serial_count_raw AS DOUBLE
                        ) AS BIGINT
                    ) AS serial_count,

                    transmission_technology,
                    message_group,
                    io_status,
                    driver_id,
                    passenger_id,

                    TRY_CAST(
                        rpm_raw AS DOUBLE
                    ) AS rpm,

                    TRY_CAST(
                        tachograph_speed_raw AS DOUBLE
                    ) AS tachograph_speed,

                    TRY_CAST(
                        tachograph_odometer_raw AS DOUBLE
                    ) AS tachograph_odometer,

                    TRY_CAST(
                        temperature_1_raw AS DOUBLE
                    ) AS temperature_1,

                    TRY_CAST(
                        temperature_2_raw AS DOUBLE
                    ) AS temperature_2,

                    TRY_CAST(
                        temperature_3_raw AS DOUBLE
                    ) AS temperature_3,

                    TRY_CAST(
                        temperature_4_raw AS DOUBLE
                    ) AS temperature_4,

                    source_file,
                    source_file_hash,
                    source_row_number,
                    row_id,
                    batch_id,
                    ingested_at,
                    ingestion_date

                FROM bronze_normalized

                WHERE regexp_full_match(
                    message_type,
                    '^T[0-9]+$'
                )

                -- T1 contém dados de identidade.
                AND message_type <> 'T1'

                -- Um evento precisa ter timestamp.
                AND COALESCE(
                    device_timestamp,
                    server_timestamp
                ) IS NOT NULL

                -- Um evento precisa identificar o dispositivo.
                AND device_serial_raw IS NOT NULL
            )

            SELECT
                STRFTIME(
                    event_timestamp,
                    '%Y-%m-%d'
                ) AS event_date,

                typed_telemetry.*,

                CASE
                    WHEN latitude IS NULL
                      OR longitude IS NULL
                    THEN FALSE

                    WHEN latitude NOT BETWEEN -90 AND 90
                      OR longitude NOT BETWEEN -180 AND 180
                    THEN FALSE

                    ELSE TRUE
                END AS has_valid_coordinates,

                CASE
                    WHEN latitude IS NULL
                      OR longitude IS NULL
                    THEN 'MISSING_COORDINATES'

                    WHEN latitude NOT BETWEEN -90 AND 90
                      OR longitude NOT BETWEEN -180 AND 180
                    THEN 'INVALID_COORDINATES'

                    WHEN hdop IS NOT NULL
                     AND hdop > 5
                    THEN 'LOW_GPS_PRECISION'

                    ELSE 'VALID'
                END AS position_quality

            FROM typed_telemetry
            """
        ).fetch_arrow_table()

        telemetry_rows_written = write_silver_table(
            telemetry_path,
            telemetry_table,
            partition_by="event_date",
            full_rebuild=full_rebuild,
        )

        # =========================================================
        # SILVER: DEVICE IDENTITY EVENTS
        # =========================================================
        print(
            "[Lakehouse][Silver] "
            "Creating device_identity_events..."
        )

        identity_table = con.execute(
            """
            WITH identity_events AS (
                SELECT
                    server_timestamp,
                    device_timestamp,

                    COALESCE(
                        device_timestamp,
                        server_timestamp
                    ) AS event_timestamp,

                    message_type,

                    TRY_CAST(
                        TRY_CAST(
                            report_type_raw AS DOUBLE
                        ) AS INTEGER
                    ) AS report_type,

                    protocol_version,
                    device_serial_raw,

                    REGEXP_REPLACE(
                        device_serial_raw,
                        '^M',
                        ''
                    ) AS device_serial,

                    battery_voltage_raw
                        AS iccid,

                    location_status_raw
                        AS identity_auxiliary,

                    latitude_raw
                        AS imsi,

                    longitude_raw
                        AS imei,

                    source_file,
                    source_file_hash,
                    source_row_number,
                    row_id,
                    batch_id,
                    ingested_at,
                    ingestion_date

                FROM bronze_normalized

                WHERE message_type = 'T1'

                AND COALESCE(
                    device_timestamp,
                    server_timestamp
                ) IS NOT NULL

                AND device_serial_raw IS NOT NULL
            )

            SELECT
                STRFTIME(
                    event_timestamp,
                    '%Y-%m-%d'
                ) AS event_date,

                identity_events.*,

                CASE
                    WHEN iccid IS NULL
                    THEN FALSE

                    WHEN regexp_full_match(
                        iccid,
                        '^[0-9]{18,22}$'
                    )
                    THEN TRUE

                    ELSE FALSE
                END AS has_valid_iccid_format,

                CASE
                    WHEN imsi IS NULL
                    THEN FALSE

                    WHEN regexp_full_match(
                        imsi,
                        '^[0-9]{14,16}$'
                    )
                    THEN TRUE

                    ELSE FALSE
                END AS has_valid_imsi_format,

                CASE
                    WHEN imei IS NULL
                    THEN FALSE

                    WHEN regexp_full_match(
                        imei,
                        '^[0-9]{15}$'
                    )
                    THEN TRUE

                    ELSE FALSE
                END AS has_valid_imei_format

            FROM identity_events
            """
        ).fetch_arrow_table()

        identity_rows_written = write_silver_table(
            identity_path,
            identity_table,
            partition_by="event_date",
            full_rebuild=full_rebuild,
        )

        # =========================================================
        # SILVER: REJECTED LOGS
        # =========================================================
        print(
            "[Lakehouse][Silver] "
            "Creating rejected_logs..."
        )

        rejected_table = con.execute(
            """
            WITH rejected AS (
                SELECT
                    COALESCE(
                        device_timestamp,
                        server_timestamp
                    ) AS event_timestamp,

                    bronze_normalized.*,

                    CASE
                        WHEN message_type IS NULL
                        THEN 'MISSING_MESSAGE_TYPE'

                        WHEN NOT regexp_full_match(
                            message_type,
                            '^T[0-9]+$'
                        )
                        THEN 'INVALID_MESSAGE_TYPE'

                        WHEN COALESCE(
                            device_timestamp,
                            server_timestamp
                        ) IS NULL
                        THEN 'MISSING_OR_INVALID_TIMESTAMP'

                        WHEN device_serial_raw IS NULL
                        THEN 'MISSING_DEVICE_SERIAL'

                        ELSE 'UNKNOWN_REJECTION_REASON'
                    END AS rejection_reason

                FROM bronze_normalized

                WHERE
                    message_type IS NULL

                    OR NOT regexp_full_match(
                        message_type,
                        '^T[0-9]+$'
                    )

                    OR COALESCE(
                        device_timestamp,
                        server_timestamp
                    ) IS NULL

                    OR device_serial_raw IS NULL
            )

            SELECT
                COALESCE(
                    STRFTIME(
                        event_timestamp,
                        '%Y-%m-%d'
                    ),
                    'unknown'
                ) AS rejection_date,

                rejected.*

            FROM rejected
            """
        ).fetch_arrow_table()

        rejected_rows_written = write_silver_table(
            rejected_path,
            rejected_table,
            partition_by="rejection_date",
            full_rebuild=full_rebuild,
        )

        mode = (
            "FULL"
            if full_rebuild
            else "INCREMENTAL"
        )

        if full_rebuild:
            result_event_dates = tuple(
                sorted(
                    {
                        *get_arrow_unique_strings(
                            telemetry_table,
                            "event_date",
                        ),
                        *get_arrow_unique_strings(
                            identity_table,
                            "event_date",
                        ),
                    }
                )
            )
            result_rejection_dates = (
                get_arrow_unique_strings(
                    rejected_table,
                    "rejection_date",
                )
            )
        else:
            result_event_dates = affected_event_dates
            result_rejection_dates = affected_rejection_dates

        print(
            "[Lakehouse][Silver] "
            f"Silver layer complete! mode={mode}"
        )
        print(
            "[Lakehouse][Silver] "
            f"Telemetry: {telemetry_path} "
            f"| rows={telemetry_table.num_rows}"
        )
        print(
            "[Lakehouse][Silver] "
            f"Identity: {identity_path} "
            f"| rows={identity_table.num_rows}"
        )
        print(
            "[Lakehouse][Silver] "
            f"Rejected: {rejected_path} "
            f"| rows={rejected_table.num_rows}"
        )

        return SilverLoadResult(
            mode=mode,
            batch_ids=normalized_batch_ids,
            affected_event_dates=result_event_dates,
            affected_rejection_dates=result_rejection_dates,
            telemetry_rows_written=telemetry_rows_written,
            identity_rows_written=identity_rows_written,
            rejected_rows_written=rejected_rows_written,
        )

    except Exception as error:
        raise RuntimeError(
            "Error while creating Silver Delta Tables: "
            f"{error}"
        ) from error

    finally:
        con.close()


if __name__ == "__main__":
    load_silver_data()