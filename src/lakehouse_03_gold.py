from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
from deltalake import DeltaTable, write_deltalake


@dataclass(frozen=True)
class GoldLoadResult:
    mode: str
    affected_event_dates: tuple[str, ...]
    affected_rejection_dates: tuple[str, ...]
    affected_devices: tuple[str, ...]
    dim_device_rows_written: int
    last_position_rows_written: int
    route_points_rows_written: int
    daily_summary_rows_written: int
    quality_summary_rows_written: int


def is_delta_table(path: Path) -> bool:
    return path.is_dir() and (path / "_delta_log").is_dir()


def load_delta_table(
    path: Path,
    table_name: str,
) -> DeltaTable:
    if not path.is_dir():
        raise FileNotFoundError(
            f"The {table_name} Delta Table directory does not exist: {path}"
        )

    if not is_delta_table(path):
        raise FileNotFoundError(
            f"The directory is not a valid Delta Table: {path}"
        )

    try:
        return DeltaTable(str(path))
    except Exception as error:
        raise RuntimeError(
            f"Could not load the {table_name} Delta Table: "
            f"{path}. Reason: {error}"
        ) from error


def get_lakehouse_paths(
    project_dir: Path,
) -> dict[str, Path]:
    silver_path = (
        project_dir
        / "data"
        / "lakehouse"
        / "02_silver"
    )

    gold_path = (
        project_dir
        / "data"
        / "lakehouse"
        / "03_gold"
    )

    return {
        "silver": silver_path,
        "telemetry": silver_path / "telemetry_events",
        "identity": silver_path / "device_identity_events",
        "rejected": silver_path / "rejected_logs",
        "gold": gold_path,
        "dim_device": gold_path / "dim_device",
        "last_position": gold_path / "device_last_position",
        "route_points": gold_path / "device_route_points",
        "daily_summary": gold_path / "device_daily_summary",
        "quality_summary": gold_path / "data_quality_summary",
    }


def normalize_partition_values(
    values: set[str] | list[str] | tuple[str, ...] | None,
) -> tuple[str, ...]:
    if values is None:
        return ()

    return tuple(
        sorted(
            {
                str(value).strip()
                for value in values
                if str(value).strip()
            }
        )
    )


def get_delta_field_type(
    path: Path,
    column_name: str,
) -> str | None:
    if not is_delta_table(path):
        return None

    table = DeltaTable(str(path))

    for field in table.schema().fields:
        if field.name != column_name:
            continue

        return str(
            getattr(
                field.type,
                "type",
                field.type,
            )
        ).lower()

    return None


def gold_supports_incremental_update(
    paths: dict[str, Path],
) -> bool:
    """
    Incremental só é seguro quando todas as tabelas Gold já existem.

    As três tabelas por data precisam estar particionadas com strings.
    """
    for key in (
        "dim_device",
        "last_position",
        "route_points",
        "daily_summary",
        "quality_summary",
    ):
        if not is_delta_table(paths[key]):
            return False

    expected_partitions = (
        ("route_points", "event_date"),
        ("daily_summary", "event_date"),
        ("quality_summary", "metric_date"),
    )

    for key, partition_column in expected_partitions:
        table = DeltaTable(str(paths[key]))

        if table.metadata().partition_columns != [
            partition_column
        ]:
            return False

        if get_delta_field_type(
            paths[key],
            partition_column,
        ) != "string":
            return False

    return True


def arrow_unique_strings(
    table: pa.Table,
    column_name: str,
) -> tuple[str, ...]:
    if column_name not in table.column_names:
        return ()

    return tuple(
        sorted(
            {
                str(value)
                for value in table[
                    column_name
                ].to_pylist()
                if value is not None
            }
        )
    )


def escape_delta_string_literal(
    value: str,
) -> str:
    return value.replace("'", "''")


def filter_arrow_partition(
    table: pa.Table,
    column_name: str,
    value: str,
) -> pa.Table:
    column = table[column_name]
    mask = pc.equal(
        column,
        pa.scalar(
            value,
            type=column.type,
        ),
    )
    return table.filter(mask)


def write_full_table(
    path: Path,
    table: pa.Table,
    *,
    partition_by: str | None = None,
) -> int:
    kwargs: dict[str, object] = {
        "mode": "overwrite",
        "schema_mode": "overwrite",
    }

    if partition_by is not None:
        kwargs["partition_by"] = [
            partition_by
        ]

    write_deltalake(
        path,
        table,
        **kwargs,
    )

    return table.num_rows


def replace_partitions(
    path: Path,
    table: pa.Table,
    *,
    partition_by: str,
    requested_partitions: tuple[str, ...],
) -> int:
    """
    ReplaceWhere por partição.

    Se uma partição afetada agora não produz nenhuma linha, removemos
    explicitamente a partição antiga para não deixar dado Gold obsoleto.
    """
    if not is_delta_table(path):
        raise RuntimeError(
            "Incremental Gold requires an existing table: "
            f"{path}"
        )

    rows_written = 0

    for partition_value in requested_partitions:
        escaped_value = escape_delta_string_literal(
            partition_value
        )
        predicate = (
            f"{partition_by} = '{escaped_value}'"
        )

        partition_table = filter_arrow_partition(
            table,
            partition_by,
            partition_value,
        )

        if partition_table.num_rows == 0:
            print(
                "[Lakehouse][Gold][Incremental] "
                f"Deleting empty {partition_by}="
                f"{partition_value}"
            )
            DeltaTable(str(path)).delete(
                predicate
            )
            continue

        print(
            "[Lakehouse][Gold][Incremental] "
            f"Replacing {partition_by}="
            f"{partition_value} "
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


def merge_entity_table(
    path: Path,
    table: pa.Table,
    *,
    key: str,
) -> int:
    """
    Upsert de tabelas Gold não particionadas por chave natural.
    """
    if table.num_rows == 0:
        return 0

    if not is_delta_table(path):
        return write_full_table(
            path,
            table,
        )

    delta_table = DeltaTable(str(path))

    (
        delta_table
        .merge(
            source=table,
            predicate=(
                f"target.{key} = source.{key}"
            ),
            source_alias="source",
            target_alias="target",
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )

    return table.num_rows


def register_filter_table(
    con: duckdb.DuckDBPyConnection,
    name: str,
    column_name: str,
    values: tuple[str, ...],
) -> None:
    con.register(
        name,
        pd.DataFrame(
            {
                column_name: list(values)
            }
        ),
    )


def create_gold_base_views(
    con: duckdb.DuckDBPyConnection,
) -> None:
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
                    COALESCE(imei, '__NULL__'),
                    COALESCE(imsi, '__NULL__'),
                    COALESCE(iccid, '__NULL__')
                ORDER BY
                    server_timestamp DESC NULLS LAST,
                    source_file DESC NULLS LAST
            ) = 1
        """
    )


def discover_affected_devices(
    con: duckdb.DuckDBPyConnection,
    event_dates: tuple[str, ...],
) -> tuple[str, ...]:
    if not event_dates:
        return ()

    register_filter_table(
        con,
        "affected_gold_dates",
        "event_date",
        event_dates,
    )

    dataframe = con.execute(
        """
        SELECT DISTINCT device_serial
        FROM (
            SELECT device_serial
            FROM telemetry_gold_base
            INNER JOIN affected_gold_dates
                ON CAST(
                    telemetry_gold_base.event_date
                    AS VARCHAR
                ) = affected_gold_dates.event_date

            UNION

            SELECT device_serial
            FROM identity_gold_base
            INNER JOIN affected_gold_dates
                ON CAST(
                    identity_gold_base.event_date
                    AS VARCHAR
                ) = affected_gold_dates.event_date
        )
        WHERE device_serial IS NOT NULL
        ORDER BY device_serial
        """
    ).df()

    return tuple(
        dataframe["device_serial"]
        .dropna()
        .astype(str)
        .tolist()
    )


def build_dim_device(
    con: duckdb.DuckDBPyConnection,
    affected_devices: tuple[str, ...] | None,
) -> pa.Table:
    if affected_devices is not None:
        register_filter_table(
            con,
            "affected_gold_devices",
            "device_serial",
            affected_devices,
        )
        device_filter = """
            WHERE devices.device_serial IN (
                SELECT device_serial
                FROM affected_gold_devices
            )
        """
    else:
        device_filter = ""

    return con.execute(
        f"""
        WITH identity_summary AS (
            SELECT
                device_serial,
                MIN(event_timestamp)
                    AS first_identity_at,
                MAX(event_timestamp)
                    AS last_identity_at,
                COUNT(*)
                    AS identity_event_count,
                ARG_MAX(imei, event_timestamp)
                    AS current_imei,
                ARG_MAX(imsi, event_timestamp)
                    AS current_imsi,
                ARG_MAX(iccid, event_timestamp)
                    AS current_iccid,
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
            GROUP BY device_serial
        ),
        telemetry_summary AS (
            SELECT
                device_serial,
                MIN(event_timestamp)
                    AS first_telemetry_at,
                MAX(event_timestamp)
                    AS last_telemetry_at,
                COUNT(*)
                    AS telemetry_event_count,
                ARG_MAX(
                    protocol_version,
                    event_timestamp
                ) AS latest_telemetry_protocol_version
            FROM telemetry_gold_base
            GROUP BY device_serial
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
                MIN(event_timestamp)
                    AS first_seen_at,
                MAX(event_timestamp)
                    AS last_seen_at
            FROM all_activity
            GROUP BY device_serial
        ),
        devices AS (
            SELECT device_serial
            FROM identity_gold_base
            UNION
            SELECT device_serial
            FROM telemetry_gold_base
        )

        SELECT
            devices.device_serial,
            identity_summary.current_imei,
            identity_summary.current_imsi,
            identity_summary.current_iccid,
            identity_summary.current_identity_auxiliary,
            COALESCE(
                identity_summary.current_protocol_version,
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
            identity_summary.current_imei_format_valid,
            identity_summary.current_imsi_format_valid,
            identity_summary.current_iccid_format_valid

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

        {device_filter}

        ORDER BY devices.device_serial
        """
    ).fetch_arrow_table()


def build_last_position(
    con: duckdb.DuckDBPyConnection,
    affected_devices: tuple[str, ...] | None,
) -> pa.Table:
    if affected_devices is not None:
        register_filter_table(
            con,
            "affected_position_devices",
            "device_serial",
            affected_devices,
        )
        device_filter = """
            AND device_serial IN (
                SELECT device_serial
                FROM affected_position_devices
            )
        """
    else:
        device_filter = ""

    return con.execute(
        f"""
        SELECT
            device_serial,
            STRFTIME(
                event_timestamp,
                '%Y-%m-%d'
            ) AS last_position_date,
            event_timestamp AS last_position_at,
            server_timestamp AS received_at,
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
          {device_filter}

        QUALIFY
            ROW_NUMBER() OVER (
                PARTITION BY device_serial
                ORDER BY
                    event_timestamp DESC,
                    server_timestamp DESC NULLS LAST,
                    serial_count DESC NULLS LAST
            ) = 1

        ORDER BY device_serial
        """
    ).fetch_arrow_table()


def build_route_points(
    con: duckdb.DuckDBPyConnection,
    event_dates: tuple[str, ...] | None,
) -> pa.Table:
    if event_dates is not None:
        register_filter_table(
            con,
            "route_gold_dates",
            "event_date",
            event_dates,
        )
        date_filter = """
            AND STRFTIME(
                event_timestamp,
                '%Y-%m-%d'
            ) IN (
                SELECT event_date
                FROM route_gold_dates
            )
        """
    else:
        date_filter = ""

    return con.execute(
        f"""
        WITH valid_points AS (
            SELECT
                STRFTIME(
                    event_timestamp,
                    '%Y-%m-%d'
                ) AS event_date,
                device_serial,
                event_timestamp,
                server_timestamp AS received_at,
                latitude,
                longitude,
                speed,
                direction_degrees,
                odometer_trip,
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
              {date_filter}
        ),
        ordered_points AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        device_serial,
                        event_date
                    ORDER BY
                        event_timestamp,
                        received_at NULLS LAST,
                        serial_count NULLS LAST
                ) AS point_sequence
            FROM valid_points
        )

        SELECT
            event_date,
            device_serial,
            point_sequence,
            event_timestamp,
            received_at,
            latitude,
            longitude,
            speed,
            direction_degrees,
            odometer_trip,
            odometer_total,
            horimeter,
            hdop,
            rx_level,
            message_type,
            report_type,
            serial_count,
            protocol_version,
            position_quality,
            COALESCE(
                speed,
                0
            ) >= 5 AS is_moving,
            source_file

        FROM ordered_points

        ORDER BY
            event_date,
            device_serial,
            point_sequence
        """
    ).fetch_arrow_table()


def build_daily_summary(
    con: duckdb.DuckDBPyConnection,
    event_dates: tuple[str, ...] | None,
) -> pa.Table:
    if event_dates is not None:
        register_filter_table(
            con,
            "daily_gold_dates",
            "event_date",
            event_dates,
        )
        date_filter = """
            WHERE STRFTIME(
                event_timestamp,
                '%Y-%m-%d'
            ) IN (
                SELECT event_date
                FROM daily_gold_dates
            )
        """
    else:
        date_filter = ""

    return con.execute(
        f"""
        WITH daily_aggregated AS (
            SELECT
                STRFTIME(
                    event_timestamp,
                    '%Y-%m-%d'
                ) AS event_date,
                device_serial,
                MIN(event_timestamp)
                    AS first_event_at,
                MAX(event_timestamp)
                    AS last_event_at,
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
            {date_filter}

            GROUP BY
                STRFTIME(
                    event_timestamp,
                    '%Y-%m-%d'
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
        ORDER BY
            event_date,
            device_serial
        """
    ).fetch_arrow_table()


def build_quality_summary(
    con: duckdb.DuckDBPyConnection,
    metric_dates: tuple[str, ...] | None,
) -> pa.Table:
    if metric_dates is not None:
        register_filter_table(
            con,
            "quality_gold_dates",
            "metric_date",
            metric_dates,
        )
        final_filter = """
            WHERE metric_date IN (
                SELECT metric_date
                FROM quality_gold_dates
            )
        """
    else:
        final_filter = ""

    return con.execute(
        f"""
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
                    telemetry_counts.telemetry_event_count,
                    0
                ) AS telemetry_event_count,
                COALESCE(
                    identity_counts.identity_event_count,
                    0
                ) AS identity_event_count,
                COALESCE(
                    rejected_counts.rejected_event_count,
                    0
                ) AS rejected_event_count,
                COALESCE(
                    rejected_counts.missing_message_type_count,
                    0
                ) AS missing_message_type_count,
                COALESCE(
                    rejected_counts.invalid_message_type_count,
                    0
                ) AS invalid_message_type_count,
                COALESCE(
                    rejected_counts.invalid_timestamp_count,
                    0
                ) AS invalid_timestamp_count,
                COALESCE(
                    rejected_counts.missing_device_serial_count,
                    0
                ) AS missing_device_serial_count,
                COALESCE(
                    rejected_counts.unknown_rejection_count,
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
        {final_filter}
        ORDER BY metric_date
        """
    ).fetch_arrow_table()


def load_gold_data(
    project_dir: Path | None = None,
    affected_event_dates: (
        set[str]
        | list[str]
        | tuple[str, ...]
        | None
    ) = None,
    affected_rejection_dates: (
        set[str]
        | list[str]
        | tuple[str, ...]
        | None
    ) = None,
) -> GoldLoadResult:
    """
    Atualiza a Gold.

    Sem datas: rebuild completo.
    Com datas: atualização incremental quando a Gold existente já
    estiver no schema da Sprint 8. Caso contrário, faz um rebuild
    completo uma única vez.
    """
    if project_dir is None:
        project_dir = (
            Path(__file__).resolve().parent.parent
        )

    paths = get_lakehouse_paths(project_dir)

    event_dates = normalize_partition_values(
        affected_event_dates
    )
    rejection_dates = normalize_partition_values(
        affected_rejection_dates
    )

    requested_incremental = (
        affected_event_dates is not None
        or affected_rejection_dates is not None
    )

    if requested_incremental and not (
        event_dates or rejection_dates
    ):
        return GoldLoadResult(
            mode="NOOP",
            affected_event_dates=(),
            affected_rejection_dates=(),
            affected_devices=(),
            dim_device_rows_written=0,
            last_position_rows_written=0,
            route_points_rows_written=0,
            daily_summary_rows_written=0,
            quality_summary_rows_written=0,
        )

    telemetry_table = load_delta_table(
        paths["telemetry"],
        "telemetry_events",
    )
    identity_table = load_delta_table(
        paths["identity"],
        "device_identity_events",
    )
    rejected_table = load_delta_table(
        paths["rejected"],
        "rejected_logs",
    )

    paths["gold"].mkdir(
        parents=True,
        exist_ok=True,
    )

    incremental_supported = (
        gold_supports_incremental_update(
            paths
        )
    )

    full_rebuild = (
        not requested_incremental
        or not incremental_supported
    )

    if requested_incremental and not incremental_supported:
        print(
            "[Lakehouse][Gold][Migration] "
            "Gold tables are not ready for Sprint 8 incremental "
            "updates. Running one full rebuild."
        )

    con = duckdb.connect()

    try:
        con.register(
            "silver_telemetry",
            telemetry_table.to_pyarrow_dataset(),
        )
        con.register(
            "silver_identity",
            identity_table.to_pyarrow_dataset(),
        )
        con.register(
            "silver_rejected",
            rejected_table.to_pyarrow_dataset(),
        )

        create_gold_base_views(con)

        if full_rebuild:
            affected_devices: tuple[str, ...] = ()
            dim_device_table = build_dim_device(
                con,
                None,
            )
            last_position_table = build_last_position(
                con,
                None,
            )
            route_points_table = build_route_points(
                con,
                None,
            )
            daily_summary_table = build_daily_summary(
                con,
                None,
            )
            quality_summary_table = build_quality_summary(
                con,
                None,
            )

            dim_rows = write_full_table(
                paths["dim_device"],
                dim_device_table,
            )
            last_rows = write_full_table(
                paths["last_position"],
                last_position_table,
            )
            route_rows = write_full_table(
                paths["route_points"],
                route_points_table,
                partition_by="event_date",
            )
            daily_rows = write_full_table(
                paths["daily_summary"],
                daily_summary_table,
                partition_by="event_date",
            )
            quality_rows = write_full_table(
                paths["quality_summary"],
                quality_summary_table,
                partition_by="metric_date",
            )

            result_event_dates = arrow_unique_strings(
                route_points_table,
                "event_date",
            )
            result_rejection_dates = tuple(
                sorted(
                    set(
                        arrow_unique_strings(
                            quality_summary_table,
                            "metric_date",
                        )
                    )
                )
            )
            mode = "FULL"

        else:
            affected_devices = discover_affected_devices(
                con,
                event_dates,
            )

            dim_device_table = build_dim_device(
                con,
                affected_devices,
            )
            last_position_table = build_last_position(
                con,
                affected_devices,
            )
            route_points_table = build_route_points(
                con,
                event_dates,
            )
            daily_summary_table = build_daily_summary(
                con,
                event_dates,
            )

            quality_dates = tuple(
                sorted(
                    set(event_dates)
                    | set(rejection_dates)
                )
            )
            quality_summary_table = build_quality_summary(
                con,
                quality_dates,
            )

            dim_rows = merge_entity_table(
                paths["dim_device"],
                dim_device_table,
                key="device_serial",
            )
            last_rows = merge_entity_table(
                paths["last_position"],
                last_position_table,
                key="device_serial",
            )
            route_rows = replace_partitions(
                paths["route_points"],
                route_points_table,
                partition_by="event_date",
                requested_partitions=event_dates,
            )
            daily_rows = replace_partitions(
                paths["daily_summary"],
                daily_summary_table,
                partition_by="event_date",
                requested_partitions=event_dates,
            )
            quality_rows = replace_partitions(
                paths["quality_summary"],
                quality_summary_table,
                partition_by="metric_date",
                requested_partitions=quality_dates,
            )

            result_event_dates = event_dates
            result_rejection_dates = rejection_dates
            mode = "INCREMENTAL"

        result = GoldLoadResult(
            mode=mode,
            affected_event_dates=result_event_dates,
            affected_rejection_dates=result_rejection_dates,
            affected_devices=affected_devices,
            dim_device_rows_written=dim_rows,
            last_position_rows_written=last_rows,
            route_points_rows_written=route_rows,
            daily_summary_rows_written=daily_rows,
            quality_summary_rows_written=quality_rows,
        )

        print(
            "[Lakehouse][Gold] "
            f"Gold layer complete! mode={result.mode}"
        )
        print(
            "[Lakehouse][Gold] "
            f"devices={len(result.affected_devices)} "
            f"| event_dates={result.affected_event_dates} "
            f"| rejection_dates="
            f"{result.affected_rejection_dates}"
        )

        return result

    except duckdb.Error as error:
        raise RuntimeError(
            "DuckDB error while creating Gold data: "
            f"{error}"
        ) from error

    except Exception as error:
        raise RuntimeError(
            "Error while updating Gold Delta Tables: "
            f"{error}"
        ) from error

    finally:
        con.close()


if __name__ == "__main__":
    load_gold_data()