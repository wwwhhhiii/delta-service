import os
from typing import Dict, Any, Union
from dataclasses import dataclass, asdict
import pathlib

from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import AsyncEngine


load_dotenv()


@dataclass
class AppSettings:
    SERVICE_NAME: str
    XLSX_INPUT_DIR: pathlib.Path
    XLSX_DIR_CHECK_INTERVAL_SEC: int = 60
    DEBUG: bool = False


@dataclass
class DatabaseSettings:
    CONN_STR: str
    ENGINE: Union[AsyncEngine, None] = None


@dataclass
class Settings:
    APP: AppSettings
    DB: DatabaseSettings

    def asdict(self) -> Dict[str, Any]:
        """Convenience method"""

        return asdict(self)


def get_settings() -> Settings:
    return Settings(
        APP=AppSettings(
            SERVICE_NAME=os.getenv("FLASK_APP_NAME"),  
            DEBUG=True if os.getenv("DEBUG").lower() == "true" else False,
            XLSX_INPUT_DIR=pathlib.Path(os.getenv("XLSX_INPUT_DIR")),
            XLSX_DIR_CHECK_INTERVAL_SEC=int(os.getenv("XLSX_DIR_CHECK_INTERVAL_SEC")),
        ),
        DB=DatabaseSettings(
            CONN_STR=f"postgresql+asyncpg://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@127.0.0.1:5432/{os.getenv('DB_NAME')}",
            ENGINE=None,
        ),
    )
