from typing import Sequence, List

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import insert, select
from sqlalchemy.sql import text

from models.db.tables import Delta
from models.db.entities import DeltaRecord, DeltaRecordWithLag


async def put_delta_data(
    db_session: AsyncSession,
    delta_records: Sequence[DeltaRecord],
) -> None:
    """Put delta records data to database."""

    async with db_session.begin():
        stmt = (
            insert(Delta).values(
                [{"rep_dt": delta.rep_dt, "delta": delta.delta} for delta in delta_records]
            )
        )
        await db_session.execute(stmt)


async def get_all_delta_data(
    db_session: AsyncSession,
) -> List[DeltaRecord]:
    """Возвращает все записи из таблицы `deltas`."""

    # TODO Возможное место оптимизации

    res = []

    async with db_session.begin():
        stmt = (
            select(Delta).order_by(Delta.rep_dt.asc())
        )
        data = await db_session.scalars(stmt)
        for rec in data:
            res.append(DeltaRecord(rep_dt=rec.rep_dt, delta=rec.delta))

    return res


async def get_delta_data_lag_view(
    db_session: AsyncSession,
) -> List[DeltaRecordWithLag]:
    """
    Возвращает записи представления `deltalag`.
    """

    res = []

    async with db_session.begin():
        stmt = text(
            f"""
            SELECT * FROM deltalag;
            """
        )

        data = await db_session.execute(stmt)
        for row in data:
            res.append(DeltaRecordWithLag(
                rep_dt=row[0],
                delta=row[1],
                delta_lag=row[2]))

    return res
