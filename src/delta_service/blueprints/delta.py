from typing import Dict, Union
import datetime
from dataclasses import asdict

from flask import Blueprint, current_app, request

from db.crud.delta import get_all_delta_data, get_delta_data_lag_view
from db.events import get_database_session
from models.api.schemas import DeltaGetDataFrameResponse
from settings.settings import Settings
from util.tables_processing import apply_df_lag
from util.convertors import merge_records_to_data_frame


delta_bp = Blueprint("delta-bp", __name__)


DeltaTableDict = Dict[
    str, Dict[
        str, Union[Dict[int, float], Dict[int, datetime.date]]
    ]
]


@delta_bp.get("/delta")
async def get_delta() -> DeltaTableDict:
    """
    Эндпоинт получения всех записей `delta`, 
    в т.ч. с заданным параметром `lag`. (Реализовано средствами pandas)
    """

    lag = abs(int(request.args.get("lag", default=0)))
    
    app_settings: Settings = current_app.config["APP_SETTINGS"]
    db_session = await get_database_session(app_settings.DB.ENGINE)
    
    try:
        delta_records = await get_all_delta_data(
            db_session=db_session)
    except ConnectionRefusedError:
        return "Service connection problem occured", 500

    # В целях удобства последующего предстваления записей БД в виде словаря и 
    # дальнейшей конвертации в JSON - записи конвертируются в pandas.DataFrame
    delta_df = merge_records_to_data_frame(delta_records)
    lagged_delta = apply_df_lag(delta_df, lag=lag, sort_by_date=False)
    response_data = lagged_delta.to_dict()

    # Создание схемы ответа
    response = DeltaGetDataFrameResponse(records=response_data)

    return asdict(response)


@delta_bp.get("/delta-lag-view")
async def get_delta_lag_view() -> DeltaTableDict:
    """
    Эндпоинт получения всех записей представления `deltalag`, 
    """

    app_settings: Settings = current_app.config["APP_SETTINGS"]
    db_session = await get_database_session(app_settings.DB.ENGINE)

    try:
        records = await get_delta_data_lag_view(
            db_session=db_session)
    except ConnectionRefusedError:
        return "Service connection problem occured", 500

    # В целях удобства последующего предстваления записей БД в виде словаря и 
    # дальнейшей конвертации в JSON - записи конвертируются в pandas.DataFrame
    data_df = merge_records_to_data_frame(records)
    response_data = data_df.to_dict()

    # Создание схемы ответа
    response = DeltaGetDataFrameResponse(records=response_data)

    return asdict(response)
