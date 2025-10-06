from typing import Iterator

import geojson
import requests

from src import Config
from src.application.common import BuildingHandler, logger


class OpenStreetMapService:
    __building_handler: BuildingHandler

    def __init__(self, building_handler: BuildingHandler):
        self.__building_handler = building_handler

    @property
    def building_handler(self) -> BuildingHandler:
        return self.__building_handler

    @staticmethod
    def download_pbf() -> None:
        logger.info(f"Downloading OSM-data from '{Config.OSM_PBF_URL}'")
        response = requests.get(Config.OSM_PBF_URL, stream=True)
        response.raise_for_status()

        with open(Config.OSM_FILE_PATH, "wb") as f:
            chunks = response.iter_content(chunk_size=Config.OSM_STREAMING_CHUNK_SIZE)
            for chunk in chunks:
                f.write(chunk)

        logger.info("Download completed")

    def yield_building_chunks(self, batch_size: int = 5000) -> Iterator[list[geojson.Feature]]:
        """
        Stream buildings in chunks of features.
        - If a cached GeoJSON exists, stream from it.
        - Otherwise, extract from OSM and yield while writing to disk.
        """
        if Config.OSM_BUILDINGS_GEOJSON_PATH.is_file():
            logger.info(f"GeoJSON already exists. Streaming '{Config.OSM_BUILDINGS_GEOJSON_PATH.name}' in chunks.")
            with open(Config.OSM_BUILDINGS_GEOJSON_PATH, "r", encoding="utf-8") as f:
                data = geojson.load(f)
                batch = []
                for feature in data["features"]:
                    batch.append(feature)
                    if len(batch) >= batch_size:
                        yield batch
                        batch = []
                if batch:
                    yield batch
            return

        logger.info(f"Extracting features from OSM-dataset. This may take some time...")
        self.building_handler.apply_file(str(Config.OSM_FILE_PATH), locations=True)
        logger.info(f"Extracted {len(self.building_handler.buildings)} buildings.")

        batch = []
        with open(Config.OSM_BUILDINGS_GEOJSON_PATH, "w", encoding="utf-8") as f:
            # stream into file incrementally
            f.write('{"type": "FeatureCollection", "features": [\n')

            for idx, feature in enumerate(self.building_handler.buildings):
                if idx > 0:
                    f.write(",\n")
                geojson.dump(feature, f)

                batch.append(feature)
                if len(batch) >= batch_size:
                    yield batch
                    batch = []

            f.write("\n]}")

        if batch:
            yield batch

    def get_geojson(self) -> geojson.base.GeoJSON:
        if Config.OSM_BUILDINGS_GEOJSON_PATH.is_file():
            logger.info(f"GeoJSON already exists. Loading '{str(Config.OSM_BUILDINGS_GEOJSON_PATH.name)}' from disk.")
            with open(Config.OSM_BUILDINGS_GEOJSON_PATH, "r") as f:
                return geojson.load(f)

        logger.info(f"Extracting features from OSM-dataset. This may take some time...")
        self.building_handler.apply_file(str(Config.OSM_FILE_PATH), locations=True)
        logger.info(f"Extracted {len(self.building_handler.buildings)} buildings.")
        feature_collection = geojson.FeatureCollection(self.building_handler.buildings)

        with open(Config.OSM_BUILDINGS_GEOJSON_PATH, "w", encoding="utf-8") as f:
            geojson.dump(feature_collection, f, indent=2)

        return geojson.dumps(feature_collection, indent=2)
