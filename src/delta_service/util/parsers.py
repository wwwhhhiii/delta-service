import pandas as pd
import pathlib
import datetime


def parse_xlsx_as_data_frame(file: pathlib.Path) -> pd.DataFrame:
    """Содержит логику парсинга `.xlsx` файла."""

    # TODO возможное место оптимизации

    data = pd.read_excel(
        file,
        dtype={"Rep_dt": datetime.date, "Delta": float},
        parse_dates=["Rep_dt"],
        thousands=",",
    )
    data["Rep_dt"] = pd.to_datetime(
        data["Rep_dt"], errors="coerce", format="mixed")

    return data
