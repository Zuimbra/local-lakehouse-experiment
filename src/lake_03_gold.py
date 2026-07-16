import shutil
from pathlib import Path

import duckdb


def normalize_sql_path(path: Path) -> str:
    """
    Converte um caminho do sistema operacional para um formato
    compatível com strings SQL do DuckDB.
    """
    return path.resolve().as_posix().replace("'", "''")


def parquet_glob(path: Path) -> str:
    """
    Retorna um glob recursivo para ler todos os arquivos Parquet
    existentes dentro de um dataset particionado.
    """
    return f"{normalize_sql_path(path)}/**/*.parquet"


def validate_parquet_dataset(path: Path, dataset_name: str) -> None:
    """
    Confirma que o diretório existe e contém pelo menos um Parquet.
    """
    if not path.is_dir():
        raise FileNotFoundError(
            f"The {dataset_name} directory does not exist: {path}"
        )

    if not any(path.rglob("*.parquet")):
        raise FileNotFoundError(
            f"No Parquet files were found in {dataset_name}: {path}"
        )


def clear_output(path: Path) -> None:
    """
    Remove uma saída Gold anterior.

    Esta primeira versão trabalha com full refresh:
    cada execução reconstrói completamente os produtos Gold.
    """
    if path.is_dir():
        shutil.rmtree(path)

    elif path.is_file():
        path.unlink()


def load_gold_data() -> None:
    project_dir = Path(__file__).resolve().parent.parent

    silver_path = (
        project_dir
        / "data"
        / "lake"
        / "02_silver"
    )

    telemetry_path = (
        silver_path
        / "telemetry_events"
    )

    identity_path = (
        silver_path
        / "device_identity_events"
    )

    rejected_path = (
        silver_path
        / "rejected_logs"
    )

    gold_path = (
        project_dir
        / "data"
        / "lake"
        / "03_gold"
    )

    dim_device_path = (
        gold_path
        / "dim_device.parquet"
    )

    last_position_path = (
        gold_path
        / "device_last_position.parquet"
    )

    daily_summary_path = (
        gold_path
        / "device_daily_summary"
    )

    quality_summary_path = (
        gold_path
        / "data_quality_summary"
    )

    # ---------------------------------------------------------
    # Validação das entradas Silver
    # ---------------------------------------------------------

    validate_parquet_dataset(
        telemetry_path,
        "telemetry_events",
    )

    validate_parquet_dataset(
        identity_path,
        "device_identity_events",
    )

    validate_parquet_dataset(
        rejected_path,
        "rejected_logs",
    )

    gold_path.mkdir(
        parents=True,
        exist_ok=True,
    )

    # ---------------------------------------------------------
    # Full refresh das saídas Gold
    # ---------------------------------------------------------

    clear_output(dim_device_path)
    clear_output(last_position_path)
    clear_output(daily_summary_path)
    clear_output(quality_summary_path)

    telemetry_sql_glob = parquet_glob(telemetry_path)
    identity_sql_glob = parquet_glob(identity_path)
    rejected_sql_glob = parquet_glob(rejected_path)

    dim_device_sql_path = normalize_sql_path(
        dim_device_path
    )

    last_position_sql_path = normalize_sql_path(
        last_position_path
    )

    daily_summary_sql_path = normalize_sql_path(
        daily_summary_path
    )

    quality_summary_sql_path = normalize_sql_path(
        quality_summary_path
    )

    print("[Lake] Reading Silver datasets...")

    con = duckdb.connect()

    try:
        # =====================================================
        # VIEWS DAS ENTRADAS SILVER
        # =====================================================

        con.execute(
            f"""
            CREATE OR REPLACE TEMP VIEW silver_telemetry AS

            SELECT *
            FROM read_parquet(
                '{telemetry_sql_glob}',
                hive_partitioning = TRUE,
                union_by_name = TRUE
            )
            """
        )

        con.execute(
            f"""
            CREATE OR REPLACE TEMP VIEW silver_identity AS

            SELECT *
            FROM read_parquet(
                '{identity_sql_glob}',
                hive_partitioning = TRUE,
                union_by_name = TRUE
            )
            """
        )

        con.execute(
            f"""
            CREATE OR REPLACE TEMP VIEW silver_rejected AS

            SELECT *
            FROM read_parquet(
                '{rejected_sql_glob}',
                hive_partitioning = TRUE,
                union_by_name = TRUE
            )
            """
        )

        # =====================================================
        # BASE DE TELEMETRIA PARA A GOLD
        # =====================================================
        #
        # A Silver preserva os eventos recebidos.
        #
        # Para evitar que retransmissões idênticas inflem os
        # indicadores Gold, mantemos apenas um evento para uma
        # combinação lógica de:
        #
        # - dispositivo;
        # - timestamp;
        # - tipo da mensagem;
        # - contador serial;
        # - posição;
        # - velocidade.
        #
        # Em caso de duplicação, o registro recebido mais
        # recentemente pelo servidor é mantido.
        # =====================================================

        con.execute(
            """
            CREATE OR REPLACE TEMP VIEW telemetry_gold_base AS

            SELECT *
            FROM silver_telemetry

            WHERE device_serial IS NOT NULL
              AND event_timestamp IS NOT NULL

            QUALIFY
                ROW_NUMBER() OVER (
                    PARTITION BY
                        device_serial,
                        event_timestamp,
                        message_type,

                        COALESCE(
                            CAST(serial_count AS VARCHAR),
                            '__NULL__'
                        ),

                        COALESCE(
                            CAST(latitude AS VARCHAR),
                            '__NULL__'
                        ),

                        COALESCE(
                            CAST(longitude AS VARCHAR),
                            '__NULL__'
                        ),

                        COALESCE(
                            CAST(speed AS VARCHAR),
                            '__NULL__'
                        )

                    ORDER BY
                        server_timestamp DESC NULLS LAST,
                        source_file DESC NULLS LAST
                ) = 1
            """
        )

        # =====================================================
        # BASE DE IDENTIDADE PARA A GOLD
        # =====================================================

        con.execute(
            """
            CREATE OR REPLACE TEMP VIEW identity_gold_base AS

            SELECT *
            FROM silver_identity

            WHERE device_serial IS NOT NULL
              AND event_timestamp IS NOT NULL

            QUALIFY
                ROW_NUMBER() OVER (
                    PARTITION BY
                        device_serial,
                        event_timestamp,

                        COALESCE(
                            imei,
                            '__NULL__'
                        ),

                        COALESCE(
                            imsi,
                            '__NULL__'
                        ),

                        COALESCE(
                            iccid,
                            '__NULL__'
                        )

                    ORDER BY
                        server_timestamp DESC NULLS LAST,
                        source_file DESC NULLS LAST
                ) = 1
            """
        )

        # =====================================================
        # GOLD: DIM_DEVICE
        # =====================================================
        #
        # Uma linha por dispositivo.
        #
        # Reúne:
        # - identidade mais recente;
        # - primeira aparição;
        # - última aparição;
        # - quantidade de eventos;
        # - presença ou ausência de identidade T1.
        # =====================================================

        print("[Lake] Creating dim_device...")

        con.execute(
            f"""
            COPY (
                WITH identity_summary AS (
                    SELECT
                        device_serial,

                        MIN(
                            event_timestamp
                        ) AS first_identity_at,

                        MAX(
                            event_timestamp
                        ) AS last_identity_at,

                        COUNT(*)
                            AS identity_event_count,

                        ARG_MAX(
                            imei,
                            event_timestamp
                        ) AS current_imei,

                        ARG_MAX(
                            imsi,
                            event_timestamp
                        ) AS current_imsi,

                        ARG_MAX(
                            iccid,
                            event_timestamp
                        ) AS current_iccid,

                        ARG_MAX(
                            identity_auxiliary,
                            event_timestamp
                        ) AS current_identity_auxiliary,

                        ARG_MAX(
                            protocol_version,
                            event_timestamp
                        ) AS current_protocol_version,

                        ARG_MAX(
                            has_valid_imei_format,
                            event_timestamp
                        ) AS current_imei_format_valid,

                        ARG_MAX(
                            has_valid_imsi_format,
                            event_timestamp
                        ) AS current_imsi_format_valid,

                        ARG_MAX(
                            has_valid_iccid_format,
                            event_timestamp
                        ) AS current_iccid_format_valid

                    FROM identity_gold_base

                    GROUP BY
                        device_serial
                ),

                telemetry_summary AS (
                    SELECT
                        device_serial,

                        MIN(
                            event_timestamp
                        ) AS first_telemetry_at,

                        MAX(
                            event_timestamp
                        ) AS last_telemetry_at,

                        COUNT(*)
                            AS telemetry_event_count,

                        ARG_MAX(
                            protocol_version,
                            event_timestamp
                        ) AS latest_telemetry_protocol_version

                    FROM telemetry_gold_base

                    GROUP BY
                        device_serial
                ),

                all_activity AS (
                    SELECT
                        device_serial,
                        event_timestamp
                    FROM identity_gold_base

                    UNION ALL

                    SELECT
                        device_serial,
                        event_timestamp
                    FROM telemetry_gold_base
                ),

                activity_summary AS (
                    SELECT
                        device_serial,

                        MIN(
                            event_timestamp
                        ) AS first_seen_at,

                        MAX(
                            event_timestamp
                        ) AS last_seen_at

                    FROM all_activity

                    GROUP BY
                        device_serial
                ),

                devices AS (
                    SELECT
                        device_serial
                    FROM identity_gold_base

                    UNION

                    SELECT
                        device_serial
                    FROM telemetry_gold_base
                )

                SELECT
                    devices.device_serial,

                    identity_summary.current_imei,
                    identity_summary.current_imsi,
                    identity_summary.current_iccid,

                    identity_summary
                        .current_identity_auxiliary,

                    COALESCE(
                        identity_summary
                            .current_protocol_version,

                        telemetry_summary
                            .latest_telemetry_protocol_version
                    ) AS current_protocol_version,

                    activity_summary.first_seen_at,
                    activity_summary.last_seen_at,

                    identity_summary.first_identity_at,
                    identity_summary.last_identity_at,

                    telemetry_summary.first_telemetry_at,
                    telemetry_summary.last_telemetry_at,

                    COALESCE(
                        identity_summary.identity_event_count,
                        0
                    ) AS identity_event_count,

                    COALESCE(
                        telemetry_summary.telemetry_event_count,
                        0
                    ) AS telemetry_event_count,

                    COALESCE(
                        identity_summary.identity_event_count,
                        0
                    ) > 0 AS has_identity_event,

                    COALESCE(
                        telemetry_summary.telemetry_event_count,
                        0
                    ) > 0 AS has_telemetry_event,

                    identity_summary
                        .current_imei_format_valid,

                    identity_summary
                        .current_imsi_format_valid,

                    identity_summary
                        .current_iccid_format_valid

                FROM devices

                LEFT JOIN identity_summary
                    ON devices.device_serial
                     = identity_summary.device_serial

                LEFT JOIN telemetry_summary
                    ON devices.device_serial
                     = telemetry_summary.device_serial

                LEFT JOIN activity_summary
                    ON devices.device_serial
                     = activity_summary.device_serial

                ORDER BY
                    devices.device_serial
            )
            TO '{dim_device_sql_path}'
            (
                FORMAT PARQUET,
                COMPRESSION ZSTD
            )
            """
        )

        # =====================================================
        # GOLD: DEVICE LAST POSITION
        # =====================================================
        #
        # Uma linha por dispositivo, contendo a posição válida
        # mais recente.
        #
        # Coordenadas 0,0 são excluídas porque, embora sejam
        # numericamente válidas, normalmente representam ausência
        # de localização em rastreadores.
        # =====================================================

        print("[Lake] Creating device_last_position...")

        con.execute(
            f"""
            COPY (
                SELECT
                    device_serial,

                    CAST(
                        event_timestamp AS DATE
                    ) AS last_position_date,

                    event_timestamp
                        AS last_position_at,

                    server_timestamp
                        AS received_at,

                    latitude,
                    longitude,
                    speed,
                    direction_degrees,

                    battery_voltage,
                    internal_battery,

                    odometer_total,
                    horimeter,

                    hdop,
                    rx_level,

                    message_type,
                    report_type,
                    serial_count,

                    protocol_version,
                    position_quality,
                    source_file

                FROM telemetry_gold_base

                WHERE has_valid_coordinates = TRUE

                  AND NOT (
                      latitude = 0
                      AND longitude = 0
                  )

                QUALIFY
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            device_serial

                        ORDER BY
                            event_timestamp DESC,
                            server_timestamp DESC NULLS LAST,
                            serial_count DESC NULLS LAST
                    ) = 1

                ORDER BY
                    device_serial
            )
            TO '{last_position_sql_path}'
            (
                FORMAT PARQUET,
                COMPRESSION ZSTD
            )
            """
        )

        # =====================================================
        # GOLD: DEVICE DAILY SUMMARY
        # =====================================================
        #
        # Uma linha por dispositivo e dia.
        #
        # O delta do odômetro é mantido como valor bruto porque
        # ainda não confirmamos a unidade original do protocolo.
        # =====================================================

        print("[Lake] Creating device_daily_summary...")

        con.execute(
            f"""
            COPY (
                WITH daily_aggregated AS (
                    SELECT
                        CAST(
                            event_timestamp AS DATE
                        ) AS event_date,

                        device_serial,

                        MIN(
                            event_timestamp
                        ) AS first_event_at,

                        MAX(
                            event_timestamp
                        ) AS last_event_at,

                        COUNT(*)
                            AS message_count,

                        COUNT(
                            DISTINCT message_type
                        ) AS distinct_message_type_count,

                        COUNT(*) FILTER (
                            WHERE has_valid_coordinates = TRUE
                        ) AS valid_position_count,

                        COUNT(*) FILTER (
                            WHERE has_valid_coordinates IS NOT TRUE
                        ) AS invalid_position_count,

                        COUNT(*) FILTER (
                            WHERE position_quality
                                = 'LOW_GPS_PRECISION'
                        ) AS low_gps_precision_count,

                        COUNT(*) FILTER (
                            WHERE speed >= 5
                        ) AS moving_event_count,

                        COUNT(*) FILTER (
                            WHERE speed IS NOT NULL
                              AND speed < 5
                        ) AS stopped_event_count,

                        ROUND(
                            AVG(speed),
                            3
                        ) AS average_speed,

                        ROUND(
                            AVG(speed) FILTER (
                                WHERE speed >= 5
                            ),
                            3
                        ) AS average_speed_while_moving,

                        MAX(speed)
                            AS maximum_speed,

                        ROUND(
                            AVG(hdop),
                            3
                        ) AS average_hdop,

                        MIN(hdop)
                            AS minimum_hdop,

                        MAX(hdop)
                            AS maximum_hdop,

                        MIN(battery_voltage)
                            AS minimum_battery_voltage,

                        MAX(battery_voltage)
                            AS maximum_battery_voltage,

                        ROUND(
                            AVG(battery_voltage),
                            3
                        ) AS average_battery_voltage,

                        MIN(internal_battery)
                            AS minimum_internal_battery,

                        MAX(internal_battery)
                            AS maximum_internal_battery,

                        ROUND(
                            AVG(internal_battery),
                            3
                        ) AS average_internal_battery,

                        ARG_MIN(
                            odometer_total,
                            event_timestamp
                        ) AS first_odometer_total,

                        ARG_MAX(
                            odometer_total,
                            event_timestamp
                        ) AS last_odometer_total,

                        MIN(event_timestamp) FILTER (
                            WHERE has_valid_coordinates = TRUE
                        ) AS first_valid_position_at,

                        MAX(event_timestamp) FILTER (
                            WHERE has_valid_coordinates = TRUE
                        ) AS last_valid_position_at,

                        ARG_MIN(
                            latitude,
                            event_timestamp
                        ) FILTER (
                            WHERE has_valid_coordinates = TRUE
                        ) AS first_latitude,

                        ARG_MIN(
                            longitude,
                            event_timestamp
                        ) FILTER (
                            WHERE has_valid_coordinates = TRUE
                        ) AS first_longitude,

                        ARG_MAX(
                            latitude,
                            event_timestamp
                        ) FILTER (
                            WHERE has_valid_coordinates = TRUE
                        ) AS last_latitude,

                        ARG_MAX(
                            longitude,
                            event_timestamp
                        ) FILTER (
                            WHERE has_valid_coordinates = TRUE
                        ) AS last_longitude

                    FROM telemetry_gold_base

                    GROUP BY
                        CAST(
                            event_timestamp AS DATE
                        ),
                        device_serial
                )

                SELECT
                    event_date,
                    device_serial,

                    first_event_at,
                    last_event_at,

                    message_count,
                    distinct_message_type_count,

                    valid_position_count,
                    invalid_position_count,
                    low_gps_precision_count,

                    ROUND(
                        valid_position_count
                        * 100.0
                        / NULLIF(
                            message_count,
                            0
                        ),
                        2
                    ) AS valid_position_percentage,

                    moving_event_count,
                    stopped_event_count,

                    average_speed,
                    average_speed_while_moving,
                    maximum_speed,

                    average_hdop,
                    minimum_hdop,
                    maximum_hdop,

                    minimum_battery_voltage,
                    maximum_battery_voltage,
                    average_battery_voltage,

                    minimum_internal_battery,
                    maximum_internal_battery,
                    average_internal_battery,

                    first_odometer_total,
                    last_odometer_total,

                    CASE
                        WHEN first_odometer_total IS NULL
                          OR last_odometer_total IS NULL
                        THEN NULL

                        WHEN last_odometer_total
                           < first_odometer_total
                        THEN NULL

                        ELSE
                            last_odometer_total
                            - first_odometer_total
                    END AS odometer_delta_raw,

                    CASE
                        WHEN first_odometer_total IS NULL
                          OR last_odometer_total IS NULL
                        THEN FALSE

                        WHEN last_odometer_total
                           < first_odometer_total
                        THEN TRUE

                        ELSE FALSE
                    END AS has_odometer_regression,

                    first_valid_position_at,
                    last_valid_position_at,

                    first_latitude,
                    first_longitude,

                    last_latitude,
                    last_longitude

                FROM daily_aggregated
            )
            TO '{daily_summary_sql_path}'
            (
                FORMAT PARQUET,
                COMPRESSION ZSTD,
                PARTITION_BY (event_date),
                OVERWRITE
            )
            """
        )

        # =====================================================
        # GOLD: DATA QUALITY SUMMARY
        # =====================================================
        #
        # Uma linha por data.
        #
        # Compara:
        # - eventos de telemetria aceitos;
        # - eventos de identidade aceitos;
        # - registros rejeitados;
        # - motivos de rejeição;
        # - percentual de rejeição.
        # =====================================================

        print("[Lake] Creating data_quality_summary...")

        con.execute(
            f"""
            COPY (
                WITH telemetry_counts AS (
                    SELECT
                        CAST(
                            event_date AS VARCHAR
                        ) AS metric_date,

                        COUNT(*)
                            AS telemetry_event_count

                    FROM silver_telemetry

                    GROUP BY
                        CAST(
                            event_date AS VARCHAR
                        )
                ),

                identity_counts AS (
                    SELECT
                        CAST(
                            event_date AS VARCHAR
                        ) AS metric_date,

                        COUNT(*)
                            AS identity_event_count

                    FROM silver_identity

                    GROUP BY
                        CAST(
                            event_date AS VARCHAR
                        )
                ),

                rejected_counts AS (
                    SELECT
                        COALESCE(
                            CAST(
                                rejection_date AS VARCHAR
                            ),
                            'unknown'
                        ) AS metric_date,

                        COUNT(*)
                            AS rejected_event_count,

                        COUNT(*) FILTER (
                            WHERE rejection_reason
                                = 'MISSING_MESSAGE_TYPE'
                        ) AS missing_message_type_count,

                        COUNT(*) FILTER (
                            WHERE rejection_reason
                                = 'INVALID_MESSAGE_TYPE'
                        ) AS invalid_message_type_count,

                        COUNT(*) FILTER (
                            WHERE rejection_reason
                                = 'MISSING_OR_INVALID_TIMESTAMP'
                        ) AS invalid_timestamp_count,

                        COUNT(*) FILTER (
                            WHERE rejection_reason
                                = 'MISSING_DEVICE_SERIAL'
                        ) AS missing_device_serial_count,

                        COUNT(*) FILTER (
                            WHERE rejection_reason
                                = 'UNKNOWN_REJECTION_REASON'
                        ) AS unknown_rejection_count

                    FROM silver_rejected

                    GROUP BY
                        COALESCE(
                            CAST(
                                rejection_date AS VARCHAR
                            ),
                            'unknown'
                        )
                ),

                all_dates AS (
                    SELECT metric_date
                    FROM telemetry_counts

                    UNION

                    SELECT metric_date
                    FROM identity_counts

                    UNION

                    SELECT metric_date
                    FROM rejected_counts
                ),

                combined AS (
                    SELECT
                        all_dates.metric_date,

                        COALESCE(
                            telemetry_counts
                                .telemetry_event_count,
                            0
                        ) AS telemetry_event_count,

                        COALESCE(
                            identity_counts
                                .identity_event_count,
                            0
                        ) AS identity_event_count,

                        COALESCE(
                            rejected_counts
                                .rejected_event_count,
                            0
                        ) AS rejected_event_count,

                        COALESCE(
                            rejected_counts
                                .missing_message_type_count,
                            0
                        ) AS missing_message_type_count,

                        COALESCE(
                            rejected_counts
                                .invalid_message_type_count,
                            0
                        ) AS invalid_message_type_count,

                        COALESCE(
                            rejected_counts
                                .invalid_timestamp_count,
                            0
                        ) AS invalid_timestamp_count,

                        COALESCE(
                            rejected_counts
                                .missing_device_serial_count,
                            0
                        ) AS missing_device_serial_count,

                        COALESCE(
                            rejected_counts
                                .unknown_rejection_count,
                            0
                        ) AS unknown_rejection_count

                    FROM all_dates

                    LEFT JOIN telemetry_counts
                        ON all_dates.metric_date
                         = telemetry_counts.metric_date

                    LEFT JOIN identity_counts
                        ON all_dates.metric_date
                         = identity_counts.metric_date

                    LEFT JOIN rejected_counts
                        ON all_dates.metric_date
                         = rejected_counts.metric_date
                )

                SELECT
                    metric_date,

                    telemetry_event_count,
                    identity_event_count,

                    telemetry_event_count
                    + identity_event_count
                        AS accepted_event_count,

                    rejected_event_count,

                    telemetry_event_count
                    + identity_event_count
                    + rejected_event_count
                        AS total_event_count,

                    ROUND(
                        rejected_event_count
                        * 100.0
                        / NULLIF(
                            telemetry_event_count
                            + identity_event_count
                            + rejected_event_count,
                            0
                        ),
                        4
                    ) AS rejection_percentage,

                    missing_message_type_count,
                    invalid_message_type_count,
                    invalid_timestamp_count,
                    missing_device_serial_count,
                    unknown_rejection_count

                FROM combined

                ORDER BY
                    metric_date
            )
            TO '{quality_summary_sql_path}'
            (
                FORMAT PARQUET,
                COMPRESSION ZSTD,
                PARTITION_BY (metric_date),
                OVERWRITE
            )
            """
        )

        print("[Lake] Gold layer complete!")
        print(f"[Lake] dim_device: {dim_device_path}")
        print(f"[Lake] last_position: {last_position_path}")
        print(f"[Lake] daily_summary: {daily_summary_path}")
        print(f"[Lake] quality_summary: {quality_summary_path}")

    except duckdb.Error as error:
        raise RuntimeError(
            f"DuckDB error while creating Gold data: {error}"
        ) from error

    finally:
        con.close()


if __name__ == "__main__":
    load_gold_data()