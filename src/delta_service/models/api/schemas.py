import datetime
from typing import Dict, Union
from dataclasses import dataclass


@dataclass
class DeltaGetDataFrameResponse:
    records: Dict[str, Dict[str, Union[datetime.date, Union[float, str]]]]

