from dataclasses import dataclass
import datetime


@dataclass
class DeltaInDb:
    __slots__ = ("rep_dt", "delta")

    rep_dt: datetime.date
    delta: float
