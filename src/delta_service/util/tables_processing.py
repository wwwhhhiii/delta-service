import pandas as pd


def apply_df_lag(
    data: pd.DataFrame,
    lag: int,
    sort_by_date: bool = True,
    fill_none: bool = True,
) -> pd.DataFrame:
    """
    Добавляет в `pandas.DataFrame` новую колонку `DeltaLag` и 
    сдвигает ее на `lag` строк вверх, 
    относительно остального фрейма.
    """

    if sort_by_date:
        data.sort_values(by="Rep_dt", inplace=True)
    
    data["DeltaLag"] = data["Delta"].shift(-lag)

    if fill_none:
        data.fillna(value="", inplace=True)
    
    return data