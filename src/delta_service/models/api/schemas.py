import datetime
from typing import List, Dict, Union
from dataclasses import dataclass


@dataclass
class DeltaRecord:
    rep_dt: datetime.date
    delta: float


@dataclass
class DeltaGetResponse:
    records: List[DeltaRecord]


@dataclass
class DeltaGetDataFrameResponse:
    """
    Example: 
    """

    records: Dict[str, Dict[str, Union[datetime.date, float]]]

