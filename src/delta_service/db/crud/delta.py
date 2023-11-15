from typing import Sequence, List

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import insert, select
import pandas as pd
from loguru import logger

from models.db.tables import Delta
from models.db.entities import DeltaInDb


async def put_delta_data(
    db_session: AsyncSession,
    delta_records: Sequence[DeltaInDb],
) -> None:
    """Put delta records data to database."""

    async with db_session.begin():
        stmt = (
            insert(Delta).values(
                [{"rep_dt": delta.rep_dt, "delta": delta.delta} for delta in delta_records]
            )
        )
        await db_session.execute(stmt)


async def get_delta_data(
    db_session: AsyncSession,
) -> List[DeltaInDb]:
    """Returns all records from delta table as `ScalarResult`."""

    res = []

    async with db_session.begin():
        stmt = (
            select(Delta)
        )
        data = await db_session.scalars(stmt)
        for d in data:
            res.append(DeltaInDb(rep_dt=d.rep_dt, delta=d.delta))

    return res


async def get_delta_data_as_dataFrame(
    db_session: AsyncSession,
) -> pd.DataFrame:
    """Requests delta table from database and wraps result in `pandas.DataFrame`"""

    data = await get_delta_data(db_session)
    df_data = pd.DataFrame({
        "rep_dt": [d.rep_dt for d in data],
        "delta": [d.delta for d in data],
    })
    
    return df_data
