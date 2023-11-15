import uuid
import datetime

from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import Float


class Base(DeclarativeBase):
    pass


class Delta(Base):
    __tablename__ = "deltas"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    rep_dt: Mapped[datetime.date]
    delta: Mapped[float] = mapped_column(Float(precision=2))
