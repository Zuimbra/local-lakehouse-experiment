from datetime import date, datetime
from typing import Any, Literal

from fastapi import (
    FastAPI,
    HTTPException,
    Query,
    Response,
    status,
)
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from .lakehouse_reader import (
    GOLD_DIR,
    GOLD_TABLES,
    describe_gold_table,
    get_data_quality,
    list_daily_summaries,
    list_data_quality,
    list_route_devices,
    list_route_points,
)


app = FastAPI(
    title="Local Lakehouse API",
    description=(
        "API REST para consultar a camada Gold "
        "do Lakehouse."
    ),
    version="0.5.0",
)


# ============================================================
# CORS
# ============================================================

# Permite que o dashboard executado localmente acesse a API.
#
# O Vite pode usar a porta 8080, conforme a configuração do
# dashboard, ou a porta padrão 5173.
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:8080",
        "http://127.0.0.1:8080",
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================
# Modelos: qualidade dos dados
# ============================================================


class DataQualityRecord(BaseModel):
    metric_date: str

    telemetry_event_count: int = Field(ge=0)
    identity_event_count: int = Field(ge=0)
    accepted_event_count: int = Field(ge=0)
    rejected_event_count: int = Field(ge=0)
    total_event_count: int = Field(ge=0)

    rejection_percentage: float = Field(
        ge=0,
        le=100,
    )

    missing_message_type_count: int = Field(ge=0)
    invalid_message_type_count: int = Field(ge=0)
    invalid_timestamp_count: int = Field(ge=0)
    missing_device_serial_count: int = Field(ge=0)
    unknown_rejection_count: int = Field(ge=0)


class DataQualityListResponse(BaseModel):
    count: int = Field(ge=0)
    items: list[DataQualityRecord]


# ============================================================
# Modelos: resumo diário
# ============================================================


class DailySummaryRecord(BaseModel):
    event_date: date
    device_serial: str

    first_event_at: datetime
    last_event_at: datetime

    message_count: int = Field(ge=0)
    distinct_message_type_count: int = Field(ge=0)

    valid_position_count: int = Field(ge=0)
    invalid_position_count: int = Field(ge=0)
    low_gps_precision_count: int = Field(ge=0)

    valid_position_percentage: float | None = Field(
        default=None,
        ge=0,
        le=100,
    )

    moving_event_count: int = Field(ge=0)
    stopped_event_count: int = Field(ge=0)

    average_speed: float | None = None
    average_speed_while_moving: float | None = None
    maximum_speed: float | None = None

    average_hdop: float | None = None
    minimum_hdop: float | None = None
    maximum_hdop: float | None = None

    minimum_battery_voltage: float | None = None
    maximum_battery_voltage: float | None = None
    average_battery_voltage: float | None = None

    minimum_internal_battery: float | None = None
    maximum_internal_battery: float | None = None
    average_internal_battery: float | None = None

    first_odometer_total: float | None = None
    last_odometer_total: float | None = None
    odometer_delta_raw: float | None = None

    has_odometer_regression: bool

    first_valid_position_at: datetime | None = None
    last_valid_position_at: datetime | None = None

    first_latitude: float | None = None
    first_longitude: float | None = None
    last_latitude: float | None = None
    last_longitude: float | None = None


class DailySummaryListResponse(BaseModel):
    count: int = Field(ge=0)
    items: list[DailySummaryRecord]


# ============================================================
# Modelos: rota GeoJSON
# ============================================================


class GeoJsonFeature(BaseModel):
    type: Literal["Feature"] = "Feature"
    properties: dict[str, Any]
    geometry: dict[str, Any]


class RouteGeoJsonResponse(BaseModel):
    type: Literal["FeatureCollection"] = "FeatureCollection"
    features: list[GeoJsonFeature]


# ============================================================
# Rotas operacionais
# ============================================================


@app.get(
    "/",
    include_in_schema=False,
)
def root() -> dict[str, str]:
    return {
        "name": "Local Lakehouse API",
        "version": "0.5.0",
        "health": "/health",
        "readiness": "/ready",
        "documentation": "/docs",
        "data_quality": "/api/v1/data-quality",
        "daily_summary": "/api/v1/daily-summary",
        "routes": "/api/v1/routes/{event_date}",
    }


@app.get(
    "/health",
    tags=["Operational"],
)
def health() -> dict[str, str]:
    """
    Confirma que o processo da API está funcionando.
    """

    return {
        "status": "ok",
    }


@app.get(
    "/ready",
    tags=["Operational"],
)
def readiness(
    response: Response,
) -> dict[str, Any]:
    """
    Confirma que as tabelas Delta esperadas estão disponíveis.
    """

    tables: dict[str, dict[str, Any]] = {}
    all_tables_ready = True

    for table_name in GOLD_TABLES:
        try:
            description = describe_gold_table(
                table_name
            )

            tables[table_name] = {
                "ready": True,
                "delta_version": description[
                    "delta_version"
                ],
                "active_file_count": description[
                    "active_file_count"
                ],
            }

        except Exception as error:
            all_tables_ready = False

            tables[table_name] = {
                "ready": False,
                "error": str(error),
            }

    if not all_tables_ready:
        response.status_code = (
            status.HTTP_503_SERVICE_UNAVAILABLE
        )

    return {
        "status": (
            "ready"
            if all_tables_ready
            else "not_ready"
        ),
        "gold_directory": str(GOLD_DIR),
        "tables": tables,
    }


# ============================================================
# Endpoints: qualidade dos dados
# ============================================================


@app.get(
    "/api/v1/data-quality",
    response_model=DataQualityListResponse,
    tags=["Data quality"],
)
def list_quality(
    date_from: date | None = Query(
        default=None,
        description=(
            "Data inicial no formato YYYY-MM-DD."
        ),
    ),
    date_to: date | None = Query(
        default=None,
        description=(
            "Data final no formato YYYY-MM-DD."
        ),
    ),
) -> DataQualityListResponse:
    """
    Lista os indicadores consolidados de qualidade.
    """

    if (
        date_from is not None
        and date_to is not None
        and date_from > date_to
    ):
        raise HTTPException(
            status_code=(
                status.HTTP_422_UNPROCESSABLE_ENTITY
            ),
            detail=(
                "date_from não pode ser posterior "
                "a date_to."
            ),
        )

    rows = list_data_quality(
        date_from=date_from,
        date_to=date_to,
    )

    return DataQualityListResponse(
        count=len(rows),
        items=[
            DataQualityRecord.model_validate(row)
            for row in rows
        ],
    )


@app.get(
    "/api/v1/data-quality/{metric_date}",
    response_model=DataQualityRecord,
    tags=["Data quality"],
)
def get_quality(
    metric_date: str,
) -> DataQualityRecord:
    """
    Retorna a qualidade de uma data ou dos registros unknown.
    """

    normalized_metric_date = metric_date.strip()

    if normalized_metric_date.lower() != "unknown":
        try:
            normalized_metric_date = (
                date.fromisoformat(
                    normalized_metric_date
                ).isoformat()
            )

        except ValueError as error:
            raise HTTPException(
                status_code=(
                    status.HTTP_422_UNPROCESSABLE_ENTITY
                ),
                detail=(
                    "metric_date deve estar no formato "
                    "YYYY-MM-DD ou ser 'unknown'."
                ),
            ) from error

    row = get_data_quality(
        normalized_metric_date
    )

    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                "Não existe resumo de qualidade para "
                f"metric_date={normalized_metric_date}."
            ),
        )

    return DataQualityRecord.model_validate(row)


# ============================================================
# Endpoints: resumo diário
# ============================================================


@app.get(
    "/api/v1/daily-summary",
    response_model=DailySummaryListResponse,
    tags=["Daily summary"],
)
def list_daily_summary(
    date_from: date | None = Query(
        default=None,
        description=(
            "Data inicial no formato YYYY-MM-DD."
        ),
    ),
    date_to: date | None = Query(
        default=None,
        description=(
            "Data final no formato YYYY-MM-DD."
        ),
    ),
    device_serial: str | None = Query(
        default=None,
        description=(
            "Serial do dispositivo. Opcional enquanto "
            "houver apenas um rastreador."
        ),
    ),
) -> DailySummaryListResponse:
    """
    Lista os resumos diários dos rastreadores.
    """

    if (
        date_from is not None
        and date_to is not None
        and date_from > date_to
    ):
        raise HTTPException(
            status_code=(
                status.HTTP_422_UNPROCESSABLE_ENTITY
            ),
            detail=(
                "date_from não pode ser posterior "
                "a date_to."
            ),
        )

    normalized_device_serial = (
        device_serial.strip()
        if device_serial is not None
        else None
    )

    if normalized_device_serial == "":
        normalized_device_serial = None

    rows = list_daily_summaries(
        date_from=date_from,
        date_to=date_to,
        device_serial=normalized_device_serial,
    )

    return DailySummaryListResponse(
        count=len(rows),
        items=[
            DailySummaryRecord.model_validate(row)
            for row in rows
        ],
    )


@app.get(
    "/api/v1/daily-summary/{event_date}",
    response_model=DailySummaryRecord,
    tags=["Daily summary"],
)
def get_daily_summary(
    event_date: date,
    device_serial: str | None = Query(
        default=None,
        description=(
            "Serial do dispositivo. Opcional enquanto "
            "houver apenas um rastreador."
        ),
    ),
) -> DailySummaryRecord:
    """
    Retorna o resumo de uma data específica.
    """

    normalized_device_serial = (
        device_serial.strip()
        if device_serial is not None
        else None
    )

    if normalized_device_serial == "":
        normalized_device_serial = None

    rows = list_daily_summaries(
        date_from=event_date,
        date_to=event_date,
        device_serial=normalized_device_serial,
    )

    if not rows:
        detail = (
            "Não existe resumo diário para "
            f"event_date={event_date.isoformat()}"
        )

        if normalized_device_serial is not None:
            detail += (
                " e device_serial="
                f"{normalized_device_serial}"
            )

        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"{detail}.",
        )

    if (
        len(rows) > 1
        and normalized_device_serial is None
    ):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                "Existe mais de um dispositivo nessa data. "
                "Informe o parâmetro device_serial."
            ),
        )

    return DailySummaryRecord.model_validate(
        rows[0]
    )


# ============================================================
# Endpoint: rota diária
# ============================================================


@app.get(
    "/api/v1/routes/{event_date}",
    response_model=RouteGeoJsonResponse,
    tags=["Routes"],
)
def get_route(
    event_date: date,
    device_serial: str | None = Query(
        default=None,
        description=(
            "Serial do dispositivo. Opcional enquanto "
            "houver apenas um rastreador."
        ),
    ),
) -> RouteGeoJsonResponse:
    """
    Retorna o trajeto GPS de uma data no formato GeoJSON.

    A geometria LineString usa a ordem:
    [longitude, latitude].
    """

    normalized_device_serial = (
        device_serial.strip()
        if device_serial is not None
        else None
    )

    if normalized_device_serial == "":
        normalized_device_serial = None

    available_devices = list_route_devices(
        event_date
    )

    if not available_devices:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                "Não existem pontos de rota para "
                f"event_date={event_date.isoformat()}."
            ),
        )

    if normalized_device_serial is None:
        if len(available_devices) > 1:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    "Existe mais de um dispositivo nessa data. "
                    "Informe o parâmetro device_serial."
                ),
            )

        selected_device_serial = available_devices[0]

    else:
        if normalized_device_serial not in available_devices:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=(
                    "Não existem pontos de rota para "
                    f"event_date={event_date.isoformat()} "
                    "e device_serial="
                    f"{normalized_device_serial}."
                ),
            )

        selected_device_serial = (
            normalized_device_serial
        )

    points = list_route_points(
        event_date=event_date,
        device_serial=selected_device_serial,
    )

    if len(points) < 2:
        raise HTTPException(
            status_code=(
                status.HTTP_422_UNPROCESSABLE_ENTITY
            ),
            detail=(
                "São necessários pelo menos dois pontos "
                "válidos para formar uma rota."
            ),
        )

    coordinates = [
        [
            float(point["longitude"]),
            float(point["latitude"]),
        ]
        for point in points
    ]

    first_point = points[0]
    last_point = points[-1]

    speed_values = [
        float(point["speed"])
        for point in points
        if point.get("speed") is not None
    ]

    first_timestamp = first_point.get(
        "event_timestamp"
    )
    last_timestamp = last_point.get(
        "event_timestamp"
    )

    properties = {
        "device_serial": selected_device_serial,
        "event_date": event_date.isoformat(),
        "point_count": len(points),
        "start_at": (
            first_timestamp.isoformat()
            if isinstance(
                first_timestamp,
                (date, datetime),
            )
            else (
                str(first_timestamp)
                if first_timestamp is not None
                else None
            )
        ),
        "end_at": (
            last_timestamp.isoformat()
            if isinstance(
                last_timestamp,
                (date, datetime),
            )
            else (
                str(last_timestamp)
                if last_timestamp is not None
                else None
            )
        ),
        "maximum_speed": (
            max(speed_values)
            if speed_values
            else None
        ),
    }

    return RouteGeoJsonResponse(
        features=[
            GeoJsonFeature(
                properties=properties,
                geometry={
                    "type": "LineString",
                    "coordinates": coordinates,
                },
            ),
            GeoJsonFeature(
                properties={
                    "role": "start",
                    "device_serial": (
                        selected_device_serial
                    ),
                    "event_timestamp": (
                        properties["start_at"]
                    ),
                },
                geometry={
                    "type": "Point",
                    "coordinates": coordinates[0],
                },
            ),
            GeoJsonFeature(
                properties={
                    "role": "end",
                    "device_serial": (
                        selected_device_serial
                    ),
                    "event_timestamp": (
                        properties["end_at"]
                    ),
                },
                geometry={
                    "type": "Point",
                    "coordinates": coordinates[-1],
                },
            ),
        ],
    )