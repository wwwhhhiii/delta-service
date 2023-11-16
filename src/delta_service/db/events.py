from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, AsyncEngine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

from settings.settings import Settings


def connect_to_database(settigns: Settings) -> None:
    """
    Логика подключения к БД.
    
    Подключается к базе данных (создает асинхронный движок) 
    и сохраняет подключение в `settings`.
    """

    # TODO try to change for scoped session
    settigns.DB.ENGINE = create_async_engine(
        settigns.DB.CONN_STR,
        poolclass=NullPool,  # https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html#using-multiple-asyncio-event-loops
    )


async def get_database_session(engine: AsyncEngine) -> AsyncSession:
    """
    Фабрика сессий БД.

    Создает новую асинхронную сессию.
    """
    
    Session = sessionmaker(
        bind=engine,
        class_=AsyncSession,
    )
    return Session()
