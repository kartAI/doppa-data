import json
from io import BytesIO
from typing import Any, Dict

import pandas as pd
import requests
import shapely
from duckdb import DuckDBPyConnection
from shapely.geometry import shape

from src import Config
from src.application.contracts import ICountyService, IBlobStorageService, IBytesService
from src.domain.enums import EPSGCode, StorageContainer


class CountyService(ICountyService):
    __db_context: DuckDBPyConnection
    __blob_storage_service: IBlobStorageService
    __bytes_service: IBytesService

    def __init__(
            self,
            db_context: DuckDBPyConnection,
            blob_storage_service: IBlobStorageService,
            bytes_service: IBytesService
    ):
        self.__db_context = db_context
        self.__blob_storage_service = blob_storage_service
        self.__bytes_service = bytes_service

    def get_county_ids(self) -> list[str]:
        response = requests.get(f"{Config.GEONORGE_BASE_URL}/fylker?sorter=fylkesnummer")
        response.raise_for_status()
        data = response.json()
        return [item["fylkesnummer"] for item in data]

    def get_county_polygons_by_id(self, county_id: str, epsg_code: EPSGCode) -> tuple[bytes, dict[str, Any]]:
        geometries = self.__get_county_polygons_from_blob_storage(region=county_id)

        if geometries is not None:
            return geometries

        wkb, geo_json = self.__fetch_county_polygons_from_api(region=county_id, epsg_code=epsg_code)
        self.__write_county_to_blob_storage(region=county_id, wkb=wkb, geo_json=geo_json)

        return wkb, geo_json

    @staticmethod
    def __fetch_county_polygons_from_api(region: str, epsg_code: EPSGCode) -> tuple[bytes, dict[str, Any]]:
        response = requests.get(
            f"{Config.GEONORGE_BASE_URL}/fylker/{region}/omrade?utkoordsys={epsg_code.value}"
        )
        response.raise_for_status()

        data = response.json()
        geom_data = data["omrade"]
        geom = shape(geom_data)
        wkb = shapely.to_wkb(geom)

        geo_json = {
            "type": geom_data["type"],
            "coordinates": geom_data["coordinates"]
        }

        return wkb, geo_json

    def __get_county_polygons_from_blob_storage(self, region: str) -> tuple[bytes, dict[str, Any]] | None:
        county_bytes = self.__blob_storage_service.download_file(
            container_name=StorageContainer.METADATA,
            blob_name=Config.COUNTY_FILE_NAME
        )

        if county_bytes is None:
            return None

        county_df = self.__bytes_service.convert_parquet_bytes_to_df(county_bytes)
        if not region in county_df["region"].values:
            return None

        row = county_df.loc[county_df["region"] == region].iloc[0]
        return row["wkb"], json.loads(row["json"])

    def __write_county_to_blob_storage(self, region: str, wkb: bytes, geo_json: Dict[str, Any]) -> None:
        county_bytes = self.__blob_storage_service.download_file(
            container_name=StorageContainer.METADATA,
            blob_name=Config.COUNTY_FILE_NAME
        )

        county_df = self.__bytes_service.convert_parquet_bytes_to_df(county_bytes) \
            if (county_bytes is not None and len(county_bytes) > 0) \
            else pd.DataFrame(columns=["region", "wkb", "json"])

        json_string = json.dumps(geo_json)
        if region in county_df["region"].values:
            mask = county_df["region"] == region
            county_df.loc[mask, "wkb"] = wkb
            county_df.loc[mask, "json"] = json_string
        else:
            new_row = pd.DataFrame({"region": [region], "wkb": [wkb], "json": [json_string]})
            county_df = pd.concat([county_df, new_row], ignore_index=True)

        buffer = BytesIO()
        county_df.to_parquet(buffer, index=False)
        buffer.seek(0)
        self.__blob_storage_service.upload_file(
            container_name=StorageContainer.METADATA,
            blob_name=Config.COUNTY_FILE_NAME,
            data=buffer.read()
        )
