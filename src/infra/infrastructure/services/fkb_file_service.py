import geopandas as gdf
import requests

from src import Config
from src.application.contracts import IFKBFileService


class FKBFileService(IFKBFileService):
    def download_fgb_zip_file(self, path: str) -> bytes:
        headers = {"Authorization": f"Bearer {Config.HUGGING_FACE_API_TOKEN}"}
        response = requests.get(path, headers=headers, stream=True)
        response.raise_for_status()
        return response.content

    def create_dataframe_from_zip_bytes(self, zip_bytes: list[bytes]) -> gdf.GeoDataFrame:
        raise NotImplementedError

    def open_fgb_file(self, path: str, *layers: str) -> gdf.GeoDataFrame:
        raise NotImplementedError
