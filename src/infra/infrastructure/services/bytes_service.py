from io import BytesIO

import pandas as pd

from src.application.contracts import IBytesService


class BytesService(IBytesService):
    @staticmethod
    def convert_bytes_to_df(data: bytes) -> pd.DataFrame:
        return pd.read_parquet(BytesIO(data))
