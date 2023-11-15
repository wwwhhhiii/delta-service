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
from models.db.entities import DeltaInDb


@dataclass
class XlsxFileRow:
    __slots__ = ("rep_dt", "delta")

    rep_dt: datetime.date
    delta: float


@dataclass
class XlsxFileHandler:
    """
    Contains logic for working with `.xlsx` files. From collection to database upload.
    
    `_work_flag` - flag signaling to scan `scan_dir` directory
    """

    db_engine: AsyncEngine
    failed_xlsx_dir: pathlib.Path = pathlib.Path("./failed_xlsx")
    scan_dir_interval_sec: int = 60

    _active_tasks: Set = field(default_factory=set)
    _work_flag: threading.Event = field(default_factory=threading.Event)
    _worker_thread: Union[threading.Thread, None] = None

    def start_scan(self, directory: Union[str, pathlib.Path]) -> None:
        """Starts `scan_dir` scanning for new `.xlsx` files"""

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
        Stops scanning `scan_dir` for `.xlsx` files.\n
        
        Stops worker thread, waits for it to finish current iteration.
        """

        self._work_flag.clear()
        self._worker_thread.join()
        self._worker_thread = None

    def _scan_dir(self, directory: pathlib.Path) -> None:
        """Contains logic for `.xlsx` files scanning"""

        # Wait for signal to start scanning dir
        self._work_flag.wait()

        asyncio.run(self._start_async_scan(directory))

    async def _start_async_scan(self, directory: pathlib.Path) -> None:
        """"""

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
                p_task.add_done_callback(partial(self._any_task_done_clb, file))
                self._active_tasks.add(p_task)

            await asyncio.sleep(self.scan_dir_interval_sec)

    def _any_task_done_clb(
        self,
        file: pathlib.Path,
        task: asyncio.Task,
    ) -> None:
        """Intended to be called for each task."""

        # Если Task был отменен до его завершения (например, в случае недоступности БД)
        # Удаление Task-а из активных -> повторная попытка в след. итерации
        if task.cancelled():
            self._active_tasks.remove(task)
            return

        # В зависимости от результата выполнения Task-а:
        exc = task.exception()
        if exc:
            # Непойманная/непредвиденная ошибка - перемещение файла в failed dir
            logger.error(f"Task for \"{file}\" failed - unhandled exception: \"{exc}\"")
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
        """Called when task raised no exception."""

        file.unlink()

        self._active_tasks.remove(task)
        logger.info(f"Successfully processed \"{file}\"")

    def _fail_data_upload_clb(
        self,
        file: pathlib.Path,
        task: asyncio.Task,
    ) -> None:
        """Called when data upload was failed."""

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
        """Entrypoint for working with noticed `.xlsx` file"""

        rows = self._get_xlsx_data_from_file(file)
        # Create database representation of xlsx file records
        db_records = [DeltaInDb(rep_dt=row.rep_dt, delta=row.delta) for row in rows]
        try:
            await self._upload_xlsx_files_data(db_records)
        except ...:  # TODO Some db connection error here
            raise asyncio.CancelledError

    def _get_xlsx_data_from_file(
        self,
        file: pathlib.Path,
    ) -> Sequence[XlsxFileRow]:
        """Extracts row data from .xlsx `file` and returns sequence of rows.
        
        Expects `file` to be in `.xlsx` format.
        """

        # TODO too much conversion, optimization needed
        # TODO mb put it in utils
        
        rows = []
        data = pd.read_excel(
            file,
            dtype={"Rep_dt": datetime.date, "Delta": float},
            parse_dates=["Rep_dt"],
            thousands=",",
        )
        data["Rep_dt"] = pd.to_datetime(
            data["Rep_dt"], errors="coerce", format="mixed")
        for index, row in data.iterrows():
            rows.append(XlsxFileRow(rep_dt=row["Rep_dt"], delta=row["Delta"]))

        return rows

    async def _upload_xlsx_files_data(self, deltas: List[DeltaInDb]) -> None:
        """Uploads parsed xlsx data in records to database"""

        session = await get_database_session(self.db_engine)
        await put_delta_data(session, delta_records=deltas)
