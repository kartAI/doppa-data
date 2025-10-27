import geopandas as gpd
import pandas as pd
import requests
from osmium.geom import WKBFactory
from osmium.osm import Area

from src import Config
from src.application.common import logger
from src.application.contracts import IOpenStreetMapFileService
from src.domain.enums import EPSGCode


class OpenStreetMapFileService(IOpenStreetMapFileService):
    __geom_factory: WKBFactory
    __buildings: list[dict]
    __batches: list[gpd.GeoDataFrame]

    def __init__(self):
        super().__init__()
        self.__geom_factory = WKBFactory()
        self.__buildings = []
        self.__batches = []

    @property
    def batches(self) -> list[gpd.GeoDataFrame]:
        return self.__batches

    @batches.setter
    def batches(self, batches: list[gpd.GeoDataFrame]) -> None:
        self.__batches = batches

    @staticmethod
    def download_pbf() -> None:
        if Config.OSM_FILE_PATH.is_file():
            logger.info("OSM-data have already been downloaded. Skipping download...")
            return

        logger.info(f"Downloading OSM-data from '{Config.OSM_PBF_URL}'")
        response = requests.get(Config.OSM_PBF_URL, stream=True)
        response.raise_for_status()

        with open(Config.OSM_FILE_PATH, "wb") as f:
            chunks = response.iter_content(chunk_size=Config.OSM_STREAMING_CHUNK_SIZE)
            for chunk in chunks:
                f.write(chunk)

        logger.info("Download completed")

    def area(self, area: Area) -> None:
        try:
            if "building" in area.tags:
                logger.debug(f"Processing building {area.id}")
                feature = self.__create_feature(area)
                self.__buildings.append(feature)

                if len(self.__buildings) >= Config.OSM_FEATURE_BATCH_SIZE:
                    self.create_gdf_from_batch(self.__buildings)
                    self.__buildings = []
                    logger.info(f"Created batch #{len(self.batches)}")

                logger.debug(f"Building {area.id} was successfully processed")
        except Exception as e:
            logger.warning(f"Skipping area {area.id} due to geometry error: {e}")

    def __create_feature(self, area: Area) -> dict:
        wkb_bytes = self.__geom_factory.create_multipolygon(area)

        props: dict[str, str | int | float] = dict(area.tags)
        props["id"] = area.id

        return {
            "geometry": wkb_bytes,
            **props
        }

    def post_apply_file_cleanup(self):
        if self.__buildings:
            self.create_gdf_from_batch(self.__buildings)
            self.__buildings = []
            logger.info(f"Created batch #{len(self.batches)} in cleanup step")

    def pop_batch_by_index(self, index: int) -> None:
        self.batches.pop(index)

    def create_gdf_from_batch(self, batch: list[dict], epsg_code: EPSGCode = EPSGCode.WGS84) -> None:
        dataframe = pd.DataFrame(batch)

        existing_columns = dataframe.columns.intersection(Config.OSM_COLUMNS_TO_KEEP)
        dataframe = dataframe[list(existing_columns)]

        if "building" in dataframe.columns:
            dataframe["building"] = dataframe["building"].where(
                ~dataframe["building"].astype(str).str.lower().eq("yes"),
                "unspecified"
            )

        dataframe = dataframe.rename(columns={"building": "type"})

        if "geometry" in dataframe.columns:
            dataframe = dataframe.rename(columns={"geometry": "geom_wkb"})

            dataframe["geom_wkb"] = dataframe["geom_wkb"].apply(
                lambda x: bytes.fromhex(x) if isinstance(x, str) and x[:4] == "0106" else x
            )

        geometries = gpd.GeoSeries.from_wkb(dataframe["geom_wkb"])
        gdf = gpd.GeoDataFrame(
            dataframe.drop(columns=["geom_wkb"]),
            geometry=geometries,
            crs=f"EPSG:{epsg_code.value}"
        )

        self.batches.append(gdf)
