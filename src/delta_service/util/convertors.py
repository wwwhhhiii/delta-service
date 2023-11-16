import pandas as pd
from typing import Sequence, Union, Dict

from models.db.entities import DeltaRecord, DeltaRecordWithLag


def merge_records_to_data_frame(
    records: Sequence[Union[DeltaRecord, DeltaRecordWithLag]],
    fill_none: bool = True,
) -> pd.DataFrame:
    """Сливает объектные репрезентации записей БД в `pandas.DataFrame`"""

    d = {}

    if len(records) != 0:
        if isinstance(records[0], DeltaRecord):
            d = _get_delta_records_rows(records)

        if isinstance(records[0], DeltaRecordWithLag):
            d = _get_lagged_delta_records_rows(records)
    else:
        d = _get_delta_records_rows(records=records)

    df = pd.DataFrame(d)
    if fill_none:
        df.fillna(value="", inplace=True)

    return df


def _get_delta_records_rows(records: Sequence[DeltaRecord]) -> Dict:
    """"""

    d = {}
    d["Rep_dt"] = [rec.rep_dt for rec in records]
    d["Delta"] = [rec.delta for rec in records]

    return d


def _get_lagged_delta_records_rows(
    records: Sequence[DeltaRecordWithLag]) -> Dict:
    """"""

    d = {}
    d["Rep_dt"] = [rec.rep_dt for rec in records]
    d["Delta"] = [rec.delta for rec in records]
    d["DeltaLag"] = [rec.delta_lag for rec in records]

    return d