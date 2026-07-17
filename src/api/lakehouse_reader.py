from datetime import date, datetime
from pathlib import Path
from typing import Any

from deltalake import DeltaTable


PROJECT_ROOT = Path(__file__).resolve().parents[2]

GOLD_DIR = (
    PROJECT_ROOT
    / "data"
    / "lakehouse"
    / "03_gold"
)

GOLD_TABLES = (
    "data_quality_summary",
    "device_daily_summary",
    "device_last_position",
    "device_route_points",
    "dim_device",
)

QUALITY_COUNT_COLUMNS = (
    "telemetry_event_count",
    "identity_event_count",
    "accepted_event_count",
    "rejected_event_count",
    "missing_message_type_count",
    "invalid_message_type_count",
    "invalid_timestamp_count",
    "missing_device_serial_count",
    "unknown_rejection_count",
)


def get_gold_table_path(table_name: str) -> Path:
    """
    Retorna o caminho de uma tabela conhecida da Gold.
    """

    if table_name not in GOLD_TABLES:
        available_tables = ", ".join(GOLD_TABLES)

        raise ValueError(
            f"Tabela Gold desconhecida: {table_name}. "
            f"Tabelas disponíveis: {available_tables}"
        )

    return GOLD_DIR / table_name


def validate_gold_table(table_name: str) -> Path:
    """
    Confirma que o diretório existe e é uma tabela Delta válida.
    """

    table_path = get_gold_table_path(table_name)

    if not table_path.is_dir():
        raise FileNotFoundError(
            f"Diretório da tabela não encontrado: {table_path}"
        )

    if not DeltaTable.is_deltatable(str(table_path)):
        raise ValueError(
            f"O diretório não é uma tabela Delta válida: "
            f"{table_path}"
        )

    return table_path


def open_gold_table(table_name: str) -> DeltaTable:
    """
    Abre o snapshot atual de uma tabela Delta da Gold.
    """

    table_path = validate_gold_table(table_name)

    return DeltaTable(str(table_path))


def describe_gold_table(
    table_name: str,
) -> dict[str, Any]:
    """
    Retorna informações básicas de uma tabela Delta.
    """

    delta_table = open_gold_table(table_name)
    arrow_schema = delta_table.schema().to_arrow()

    columns = [
        {
            "name": field.name,
            "type": str(field.type),
            "nullable": field.nullable,
        }
        for field in arrow_schema
    ]

    return {
        "table_name": table_name,
        "table_path": str(
            get_gold_table_path(table_name)
        ),
        "delta_version": delta_table.version(),
        "active_file_count": len(
            delta_table.file_uris()
        ),
        "columns": columns,
    }


def read_gold_table(
    table_name: str,
    columns: list[str] | None = None,
) -> list[dict[str, Any]]:
    """
    Lê o snapshot atual de uma tabela Gold.

    Para as tabelas pequenas do MVP, a leitura completa é suficiente.
    """

    delta_table = open_gold_table(table_name)
    dataset = delta_table.to_pyarrow_dataset()

    arrow_table = dataset.to_table(
        columns=columns,
    )

    return arrow_table.to_pylist()


def normalize_metric_date(value: Any) -> str:
    """
    Normaliza datas para YYYY-MM-DD.

    Exemplos:
    2026-07-01 00:00:00 -> 2026-07-01
    None ou valor inválido -> unknown
    """

    if value is None:
        return "unknown"

    if isinstance(value, datetime):
        return value.date().isoformat()

    if isinstance(value, date):
        return value.isoformat()

    text_value = str(value).strip()

    if text_value.lower() in {
        "",
        "none",
        "nan",
        "nat",
        "unknown",
    }:
        return "unknown"

    try:
        parsed_datetime = datetime.fromisoformat(
            text_value
        )

        return parsed_datetime.date().isoformat()

    except ValueError:
        return "unknown"


def list_data_quality(
    date_from: date | None = None,
    date_to: date | None = None,
) -> list[dict[str, Any]]:
    """
    Retorna os indicadores de qualidade consolidados por data.

    Registros como 2026-07-01 e 2026-07-01 00:00:00
    são agregados na mesma data.
    """

    columns = [
        "metric_date",
        *QUALITY_COUNT_COLUMNS,
    ]

    source_rows = read_gold_table(
        "data_quality_summary",
        columns=columns,
    )

    grouped_rows: dict[str, dict[str, Any]] = {}

    for source_row in source_rows:
        metric_date = normalize_metric_date(
            source_row.get("metric_date")
        )

        if metric_date not in grouped_rows:
            grouped_rows[metric_date] = {
                "metric_date": metric_date,
                **{
                    column_name: 0
                    for column_name
                    in QUALITY_COUNT_COLUMNS
                },
            }

        target_row = grouped_rows[metric_date]

        for column_name in QUALITY_COUNT_COLUMNS:
            value = source_row.get(column_name)

            target_row[column_name] += int(
                value or 0
            )

    rows: list[dict[str, Any]] = []

    for row in grouped_rows.values():
        metric_date = row["metric_date"]

        if metric_date != "unknown":
            parsed_date = date.fromisoformat(
                metric_date
            )

            if (
                date_from is not None
                and parsed_date < date_from
            ):
                continue

            if (
                date_to is not None
                and parsed_date > date_to
            ):
                continue

        elif date_from is not None or date_to is not None:
            # "unknown" não participa de filtros por período.
            continue

        total_event_count = (
            row["accepted_event_count"]
            + row["rejected_event_count"]
        )

        rejection_percentage = (
            round(
                row["rejected_event_count"]
                / total_event_count
                * 100,
                4,
            )
            if total_event_count > 0
            else 0.0
        )

        rows.append(
            {
                "metric_date": metric_date,
                "telemetry_event_count": (
                    row["telemetry_event_count"]
                ),
                "identity_event_count": (
                    row["identity_event_count"]
                ),
                "accepted_event_count": (
                    row["accepted_event_count"]
                ),
                "rejected_event_count": (
                    row["rejected_event_count"]
                ),
                "total_event_count": (
                    total_event_count
                ),
                "rejection_percentage": (
                    rejection_percentage
                ),
                "missing_message_type_count": (
                    row[
                        "missing_message_type_count"
                    ]
                ),
                "invalid_message_type_count": (
                    row[
                        "invalid_message_type_count"
                    ]
                ),
                "invalid_timestamp_count": (
                    row["invalid_timestamp_count"]
                ),
                "missing_device_serial_count": (
                    row[
                        "missing_device_serial_count"
                    ]
                ),
                "unknown_rejection_count": (
                    row["unknown_rejection_count"]
                ),
            }
        )

    dated_rows = [
        row
        for row in rows
        if row["metric_date"] != "unknown"
    ]

    unknown_rows = [
        row
        for row in rows
        if row["metric_date"] == "unknown"
    ]

    dated_rows.sort(
        key=lambda row: row["metric_date"],
        reverse=True,
    )

    return dated_rows + unknown_rows


def get_data_quality(
    metric_date: str,
) -> dict[str, Any] | None:
    """
    Busca o resumo consolidado de uma data.
    """

    target_date = (
        "unknown"
        if metric_date.lower() == "unknown"
        else date.fromisoformat(metric_date).isoformat()
    )

    rows = list_data_quality()

    for row in rows:
        if row["metric_date"] == target_date:
            return row

    return None

DAILY_SUMMARY_COLUMNS = [
    "event_date",
    "device_serial",
    "first_event_at",
    "last_event_at",
    "message_count",
    "distinct_message_type_count",
    "valid_position_count",
    "invalid_position_count",
    "low_gps_precision_count",
    "valid_position_percentage",
    "moving_event_count",
    "stopped_event_count",
    "average_speed",
    "average_speed_while_moving",
    "maximum_speed",
    "average_hdop",
    "minimum_hdop",
    "maximum_hdop",
    "minimum_battery_voltage",
    "maximum_battery_voltage",
    "average_battery_voltage",
    "minimum_internal_battery",
    "maximum_internal_battery",
    "average_internal_battery",
    "first_odometer_total",
    "last_odometer_total",
    "odometer_delta_raw",
    "has_odometer_regression",
    "first_valid_position_at",
    "last_valid_position_at",
    "first_latitude",
    "first_longitude",
    "last_latitude",
    "last_longitude",
]


def normalize_event_date(
    value: Any,
) -> date | None:
    """
    Converte o campo event_date para date.
    """

    if value is None:
        return None

    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, date):
        return value

    try:
        return datetime.fromisoformat(
            str(value).strip()
        ).date()
    except ValueError:
        return None


def list_daily_summaries(
    date_from: date | None = None,
    date_to: date | None = None,
    device_serial: str | None = None,
) -> list[dict[str, Any]]:
    """
    Lista os resumos diários da Gold.

    O device_serial é opcional enquanto houver apenas
    um dispositivo, mas continua disponível para escala.
    """

    source_rows = read_gold_table(
        "device_daily_summary",
        columns=DAILY_SUMMARY_COLUMNS,
    )

    rows: list[dict[str, Any]] = []

    for source_row in source_rows:
        event_date = normalize_event_date(
            source_row.get("event_date")
        )

        if event_date is None:
            continue

        row_device_serial = source_row.get(
            "device_serial"
        )

        if (
            device_serial is not None
            and row_device_serial != device_serial
        ):
            continue

        if (
            date_from is not None
            and event_date < date_from
        ):
            continue

        if (
            date_to is not None
            and event_date > date_to
        ):
            continue

        normalized_row = dict(source_row)
        normalized_row["event_date"] = event_date

        rows.append(normalized_row)

    rows.sort(
        key=lambda row: (
            row["event_date"],
            row["device_serial"],
        ),
        reverse=True,
    )

    return rows

ROUTE_POINT_COLUMNS = [
    "event_date",
    "device_serial",
    "point_sequence",
    "event_timestamp",
    "received_at",
    "latitude",
    "longitude",
    "speed",
    "direction_degrees",
    "odometer_trip",
    "odometer_total",
    "horimeter",
    "hdop",
    "rx_level",
    "message_type",
    "report_type",
    "serial_count",
    "protocol_version",
    "position_quality",
    "is_moving",
    "source_file",
]


def list_route_devices(
    event_date: date,
) -> list[str]:
    """
    Retorna os dispositivos que possuem pontos de rota na data.
    """

    rows = list_route_points(
        event_date=event_date,
    )

    return sorted(
        {
            str(row["device_serial"])
            for row in rows
            if row.get("device_serial") is not None
        }
    )


def list_route_points(
    event_date: date,
    device_serial: str | None = None,
) -> list[dict[str, Any]]:
    """
    Retorna os pontos GPS de uma data em ordem cronológica.

    O serial é opcional enquanto houver apenas um dispositivo.
    Quando informado, somente os pontos desse rastreador são
    retornados.
    """

    source_rows = read_gold_table(
        "device_route_points",
        columns=ROUTE_POINT_COLUMNS,
    )

    normalized_device_serial = (
        device_serial.strip()
        if device_serial is not None
        else None
    )

    if normalized_device_serial == "":
        normalized_device_serial = None

    rows: list[dict[str, Any]] = []

    for source_row in source_rows:
        row_event_date = normalize_event_date(
            source_row.get("event_date")
        )

        if row_event_date != event_date:
            continue

        row_device_serial = source_row.get(
            "device_serial"
        )

        if (
            normalized_device_serial is not None
            and row_device_serial
            != normalized_device_serial
        ):
            continue

        latitude = source_row.get("latitude")
        longitude = source_row.get("longitude")

        if latitude is None or longitude is None:
            continue

        latitude = float(latitude)
        longitude = float(longitude)

        if not -90 <= latitude <= 90:
            continue

        if not -180 <= longitude <= 180:
            continue

        if latitude == 0 and longitude == 0:
            continue

        normalized_row = dict(source_row)
        normalized_row["event_date"] = row_event_date
        normalized_row["latitude"] = latitude
        normalized_row["longitude"] = longitude

        rows.append(normalized_row)

    rows.sort(
        key=lambda row: (
            str(row.get("device_serial") or ""),
            int(row.get("point_sequence") or 0),
            row.get("event_timestamp")
            or datetime.min,
        )
    )

    return rows


if __name__ == "__main__":
    test_date = date(2026, 7, 1)

    devices = list_route_devices(test_date)
    points = list_route_points(test_date)

    print(f"Gold: {GOLD_DIR}")
    print(f"Data testada: {test_date}")
    print(f"Dispositivos: {devices}")
    print(f"Quantidade de pontos: {len(points)}")

    if points:
        print(f"Primeiro ponto: {points[0]}")
        print(f"Último ponto: {points[-1]}")