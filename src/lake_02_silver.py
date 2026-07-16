from pathlib import Path

import duckdb
import os


def normalize_sql_path(path: Path) -> str:
    """
    Converte o caminho para um formato compatível com o SQL do DuckDB.
    """
    return path.resolve().as_posix().replace("'", "''")


def load_silver_data():
    project_dir = Path(__file__).resolve().parent.parent

    # O arquivo já é resultado da ingestão Bronze.
    bronze_path = (
        project_dir
        / "data"
        / "lake"
        / "01_bronze"
        / "logs_rastreador_2026-07-01.parquet"
    )

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

    os.makedirs(silver_path, exist_ok=True)

    if not bronze_path.is_file():
        raise FileNotFoundError(
            f"The Bronze file does not exist: {bronze_path}"
        )

    bronze_sql_path = normalize_sql_path(bronze_path)
    telemetry_sql_path = normalize_sql_path(telemetry_path)
    identity_sql_path = normalize_sql_path(identity_path)
    rejected_sql_path = normalize_sql_path(rejected_path)

    print("[Lake] Reading from Bronze Parquet...")

    con = duckdb.connect()

    try:
        # ---------------------------------------------------------
        # View comum da Bronze
        # ---------------------------------------------------------
        #
        # Aqui fazemos somente normalizações comuns:
        # - nomes das colunas;
        # - remoção de espaços;
        # - strings vazias para NULL;
        # - timestamps com TRY_CAST.
        #
        # Os campos BAT_VOLT, LAT e LONT continuam como texto nesta
        # etapa porque, na mensagem T1, possuem outro significado.
        # ---------------------------------------------------------

        con.execute(f"""
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

                filename AS source_file

            FROM read_parquet(
                '{bronze_sql_path}',
                filename = TRUE
            )
        """)

        # ---------------------------------------------------------
        # Silver: Telemetry Events
        # ---------------------------------------------------------

        print("[Lake] Creating telemetry_events...")

        con.execute(f"""
            COPY (
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

                        source_file

                    FROM bronze_normalized

                    WHERE regexp_full_match(
                        message_type,
                        '^T[0-9]+$'
                    )

                    -- T1 tem outro schema lógico.
                    AND message_type <> 'T1'

                    -- Um evento precisa ter data e dispositivo.
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
            )
            TO '{telemetry_sql_path}'
            (
                FORMAT PARQUET,
                COMPRESSION ZSTD,
                PARTITION_BY (event_date),
                OVERWRITE TRUE
            )
        """)

        # ---------------------------------------------------------
        # Silver: Device Identity Events
        # ---------------------------------------------------------
        #
        # Não agrupamos por dispositivo.
        # A Silver preserva todas as mensagens T1.
        # A dim_device será criada depois, na Gold.
        # ---------------------------------------------------------

        print("[Lake] Creating device_identity_events...")

        con.execute(f"""
            COPY (
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

                        source_file

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
                            '^[0-9]{{18,22}}$'
                        )
                        THEN TRUE

                        ELSE FALSE
                    END AS has_valid_iccid_format,

                    CASE
                        WHEN imsi IS NULL
                        THEN FALSE

                        WHEN regexp_full_match(
                            imsi,
                            '^[0-9]{{14,16}}$'
                        )
                        THEN TRUE

                        ELSE FALSE
                    END AS has_valid_imsi_format,

                    CASE
                        WHEN imei IS NULL
                        THEN FALSE

                        WHEN regexp_full_match(
                            imei,
                            '^[0-9]{{15}}$'
                        )
                        THEN TRUE

                        ELSE FALSE
                    END AS has_valid_imei_format

                FROM identity_events
            )
            TO '{identity_sql_path}'
            (
                FORMAT PARQUET,
                COMPRESSION ZSTD,
                PARTITION_BY (event_date),
                OVERWRITE TRUE
            )
        """)

        # ---------------------------------------------------------
        # Silver: Rejected Logs
        # ---------------------------------------------------------

        print("[Lake] Creating rejected_logs...")

        con.execute(f"""
            COPY (
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
            )
            TO '{rejected_sql_path}'
            (
                FORMAT PARQUET,
                COMPRESSION ZSTD,
                PARTITION_BY (rejection_date),
                OVERWRITE TRUE
            )
        """)

        print("[Lake] Silver layer complete!")
        print(f"[Lake] Telemetry: {telemetry_path}")
        print(f"[Lake] Identity: {identity_path}")
        print(f"[Lake] Rejected: {rejected_path}")

    except duckdb.Error as error:
        raise RuntimeError(
            f"DuckDB error while creating Silver data: {error}"
        ) from error

    finally:
        con.close()


if __name__ == "__main__":
    load_silver_data()