from __future__ import annotations

from pathlib import Path

import duckdb
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


def write_silver_table(
    path: Path,
    dataframe,
    *,
    partition_by: str,
) -> None:
    """
    Faz rebuild completo de uma tabela Silver.

    Sprint 6 ainda não é incremental. schema_mode='overwrite' é
    necessário porque as tabelas Silver antigas não possuíam todos
    os metadados de linhagem adicionados nesta sprint.
    """
    write_deltalake(
        path,
        dataframe,
        mode="overwrite",
        schema_mode="overwrite",
        partition_by=[partition_by],
    )


def load_silver_data(
    project_dir: Path | None = None,
) -> None:
    """
    Reconstrói a Silver inteira a partir da Bronze consolidada.

    Nesta sprint:
    - Bronze: múltiplos arquivos + ingestão incremental;
    - Silver: rebuild completo;
    - Gold: rebuild completo.

    O parâmetro project_dir existe principalmente para facilitar
    testes automatizados. O pipeline continua chamando a função sem
    argumentos.
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

            FROM bronze
            """
        )

        # =========================================================
        # SILVER: TELEMETRY EVENTS
        # =========================================================
        print(
            "[Lakehouse][Silver] "
            "Creating telemetry_events..."
        )

        df_telemetry = con.execute(
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
                CAST(
                    event_timestamp AS DATE
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
        ).df()

        write_silver_table(
            telemetry_path,
            df_telemetry,
            partition_by="event_date",
        )

        # =========================================================
        # SILVER: DEVICE IDENTITY EVENTS
        # =========================================================
        print(
            "[Lakehouse][Silver] "
            "Creating device_identity_events..."
        )

        df_identity = con.execute(
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
                CAST(
                    event_timestamp AS DATE
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
        ).df()

        write_silver_table(
            identity_path,
            df_identity,
            partition_by="event_date",
        )

        # =========================================================
        # SILVER: REJECTED LOGS
        # =========================================================
        print(
            "[Lakehouse][Silver] "
            "Creating rejected_logs..."
        )

        df_rejected = con.execute(
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
        ).df()

        write_silver_table(
            rejected_path,
            df_rejected,
            partition_by="rejection_date",
        )

        print(
            "[Lakehouse][Silver] "
            "Silver layer complete!"
        )
        print(
            "[Lakehouse][Silver] "
            f"Telemetry: {telemetry_path} "
            f"| rows={len(df_telemetry)}"
        )
        print(
            "[Lakehouse][Silver] "
            f"Identity: {identity_path} "
            f"| rows={len(df_identity)}"
        )
        print(
            "[Lakehouse][Silver] "
            f"Rejected: {rejected_path} "
            f"| rows={len(df_rejected)}"
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