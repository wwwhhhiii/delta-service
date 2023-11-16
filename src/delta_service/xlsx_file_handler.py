import asyncio
import threading
import datetime
import pathlib
from dataclasses import dataclass, field
from typing import Union, List, Sequence, Set
from functools import partial

from sqlalchemy.ext.asyncio import AsyncEngine
import pandas as pd
from loguru import logger

from db.events import get_database_session
from db.crud.delta import put_delta_data
from models.db.entities import DeltaRecord
from util.parsers import parse_xlsx_as_data_frame


@dataclass
class XlsxFileRow:
    __slots__ = ("rep_dt", "delta")

    rep_dt: datetime.date
    delta: float


@dataclass
class XlsxFileHandler:
    """
    Содержит в себе логику работы с `.xlsx` файлами.
    
    Ответственнен за:
    - Обнаружение новых `.xlsx` файлов
    - Парсинг и валидацию файлов
    - Загрузку содержимого файлов в БД 
    
    `db_engine` - Асинхронный движок, для создания сессий в БД.
    `failed_xlsx_dir` - Директория для "необрабатываемых" .xlsx файлов.
    `scan_dir_interval_sec` - Интервал проверки директории с `.xlsx` файлами на наличие новых файлов.
    """

    db_engine: AsyncEngine
    failed_xlsx_dir: pathlib.Path = pathlib.Path("./failed_xlsx")
    scan_dir_interval_sec: int = 60

    # Для хранения ссылок на активные задачи работы с .xlsx файлами
    _active_tasks: Set = field(default_factory=set)
    # Флаг регуляции процесса работы
    _work_flag: threading.Event = field(default_factory=threading.Event)
    _worker_thread: Union[threading.Thread, None] = None

    def start_scan(self, directory: Union[str, pathlib.Path]) -> None:
        """Метод запуска обработчика `.xlsx` файлов."""

        directory = pathlib.Path(directory)

        if not directory.exists() or not directory.is_dir():
            raise ValueError("Scan directory does not exist or is not a directory")

        # If scanning thread is already running
        if self._worker_thread is not None:
            return

        self._worker_thread = threading.Thread(
            target=self._scan_dir, args=(directory,))
        self._worker_thread.start()
        self._work_flag.set()

    def stop_scan(self) -> None:
        """
        Останавливает ожидание в `.xlsx` файлов в `scan_dir`.\n
        
        Останавливает рабочий поток, ожидает пока он завершит текущую итерацию.
        """

        self._work_flag.clear()
        self._worker_thread.join()
        self._worker_thread = None

    def _scan_dir(self, directory: pathlib.Path) -> None:
        """Инициализирует процесс асинхронного ожидания `.xlsx` файлов."""

        # Wait for signal to start scanning dir
        self._work_flag.wait()

        asyncio.run(self._start_async_scan(directory))

    async def _start_async_scan(self, directory: pathlib.Path) -> None:
        """Точка входя для асинхронной обработки `.xlsx` файлов."""

        logger.debug("xlsx file hanler is started")

        # Work until termination signal from parent thread
        while self._work_flag.isSet():
            xlsx_to_process = []

            # Collect accumulated .xlsx files
            for entity in directory.iterdir():
                if entity.is_file() and entity.suffix.lower() == ".xlsx":
                    xlsx_to_process.append(entity)

            # Create parse-upload task for each collected file
            for file in xlsx_to_process:
                p_task = asyncio.Task(self._process_xlsx_file(file))
                p_task.add_done_callback(
                    partial(self._any_task_done_clb, file))
                self._active_tasks.add(p_task)

            await asyncio.sleep(self.scan_dir_interval_sec)

    def _any_task_done_clb(
        self,
        file: pathlib.Path,
        task: asyncio.Task,
    ) -> None:
        """Вызывается по завершению работы каждого Task-а обработки файла."""

        # Если Task был отменен до его завершения (например, в случае недоступности БД)
        # Удаление Task-а из активных -> повторная попытка в след. итерации
        if task.cancelled():
            self._active_tasks.remove(task)
            return

        # В зависимости от результата выполнения Task-а:
        exc = task.exception()
        if exc:
            # Непойманная/непредвиденная ошибка - перемещение файла в failed dir
            logger.error(
                f"Task for \"{file}\" failed - unhandled exception: \"{exc}\"")
            task.add_done_callback(
                partial(self._fail_data_upload_clb, file))
        else:
            # Удачное завершение Task-а - удаление файла
            task.add_done_callback(
                partial(self._success_data_upload_clb, file))
        
    def _success_data_upload_clb(
        self,
        file: pathlib.Path,
        task: asyncio.Task,
    ) -> None:
        """
        Вызывается при удачном завершении Task-a по загрузке файла в БД.
        
        Удаляет обработанный файл.
        """

        file.unlink()

        self._active_tasks.remove(task)
        logger.info(f"Successfully processed \"{file}\"")

    def _fail_data_upload_clb(
        self,
        file: pathlib.Path,
        task: asyncio.Task,
    ) -> None:
        """
        Вызывается при неудачной попытке загрузки данных файла в БД.
        
        Переносит файл в директорию файлоф, которые вызвали непредусмотренную ошибку.
        """

        if not self.failed_xlsx_dir.exists():
            self.failed_xlsx_dir.mkdir()

        # Перемещение файла в filed directory
        file.rename(self.failed_xlsx_dir / file.name)
        
        self._active_tasks.remove(task)
        logger.error(
            f"Failed to process \"{file}\","
            f" moved to \"{self.failed_xlsx_dir.absolute()}\""
        )

    async def _process_xlsx_file(self, file: pathlib.Path) -> None:
        """Входня точка для обработки нового `.xlsx` файла."""

        rows = self._get_xlsx_data_from_file(file)
        # Создание репрезентаций записей содержимого .xslx файлов в БД
        db_records = [
            DeltaRecord(rep_dt=row.rep_dt, delta=row.delta) for row in rows]
        try:
            await self._upload_xlsx_files_data(db_records)
        except ConnectionRefusedError:
            logger.error(
                "Connection error occured while uploading .xlsx data,"
                " cancelling task"
            )
            raise asyncio.CancelledError

    def _get_xlsx_data_from_file(
        self,
        file: pathlib.Path,
    ) -> Sequence[XlsxFileRow]:
        """
        Извлекает данные из `.xlsx` файла и возвращает список записей.
        
        Ожидает, что `file` будет в `.xlsx` формате.
        """
        
        rows = []
        data = parse_xlsx_as_data_frame(file)
        for index, row in data.iterrows():
            rows.append(XlsxFileRow(rep_dt=row["Rep_dt"], delta=row["Delta"]))

        return rows

    async def _upload_xlsx_files_data(self, deltas: List[DeltaRecord]) -> None:
        """Загружает извлеченные записи в БД."""

        session = await get_database_session(self.db_engine)
        await put_delta_data(session, delta_records=deltas)
