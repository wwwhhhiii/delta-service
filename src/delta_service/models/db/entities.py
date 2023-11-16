from dataclasses import dataclass
import datetime
from typing import Union


@dataclass
class DeltaRecord:
    __slots__ = ("rep_dt", "delta")

    rep_dt: datetime.date
    delta: float


@dataclass
class DeltaRecordWithLag:
    __slots__ = ("rep_dt", "delta", "delta_lag")

    rep_dt: datetime.date
    delta: float
    delta_lag: Union[float, None]
