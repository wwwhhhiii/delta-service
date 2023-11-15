from typing import Dict, Union
import datetime
from dataclasses import asdict

from flask import Blueprint, current_app

from db.crud.delta import get_delta_data_as_dataFrame
from db.events import get_database_session
from models.api.schemas import DeltaGetDataFrameResponse
from settings.settings import Settings


delta_bp = Blueprint("delta-bp", __name__)


@delta_bp.get("/delta-df")
async def get_delta() -> Dict[str, Dict[str, Union[float, datetime.date]]]:
    """"""
    
    app_settings: Settings = current_app.config["APP_SETTINGS"]
    db_session = await get_database_session(app_settings.DB.ENGINE)
    
    try:
        delta_records = await get_delta_data_as_dataFrame(db_session=db_session)
    except ConnectionRefusedError:
        return "Service problem occured", 500
    
    response_data = delta_records.to_dict()
    response = DeltaGetDataFrameResponse(records=response_data)

    return asdict(response)
