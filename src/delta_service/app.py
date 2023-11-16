from flask import Flask

from blueprints import delta
from xlsx_file_handler import XlsxFileHandler
from db.events import connect_to_database
from settings.settings import Settings, get_settings


def create_app(settings: Settings) -> Flask:
    """Фабрика приложения Flask"""

    app = Flask(settings.APP.SERVICE_NAME)

    # Update flask app config with settings
    # mainly for usage in blueprints
    app.config["APP_SETTINGS"] = settings
    
    # Register flask routes
    app.register_blueprint(delta.delta_bp)

    return app


def create_xlsx_file_handler(
    settings: Settings,
) -> XlsxFileHandler:
    """Создает объект, ответственный за работу с `.xlsx` файлами."""

    fh = XlsxFileHandler(
        scan_dir_interval_sec=settings.APP.XLSX_DIR_CHECK_INTERVAL_SEC,
        db_engine=settings.DB.ENGINE,
    )

    return fh


if __name__ == "__main__":
    settings = get_settings()
    connect_to_database(settings)

    file_handler = create_xlsx_file_handler(settings)
    # Сканирование директории и ожидание новых .xlsx файлов
    # в отдельном потоке со своим asyncio event loop
    file_handler.start_scan(directory=settings.APP.XLSX_INPUT_DIR)

    app = create_app(settings)
    app.run(debug=settings.APP.DEBUG)
