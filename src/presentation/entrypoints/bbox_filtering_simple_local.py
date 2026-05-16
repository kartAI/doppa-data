import geopandas as gpd
from dependency_injector.wiring import Provide, inject

from src import Config
from src.application.common.monitor import monitor
from src.application.contracts import IBlobStorageService
from src.application.dtos import CostConfiguration
from src.domain.enums import StorageContainer, BenchmarkIteration
from src.infra.infrastructure import Containers


def bbox_filtering_simple_local() -> None:
    """
    Benchmark: simple Oslo-area bounding-box filter over the buildings shapefile,
    executed locally via GeoPandas. Downloads the pre-baked shapefile copy from
    blob storage before running the timed reprojection and area-filter pipeline.
    """
    _download_data()
    _benchmark()


@monitor(
    query_id="bbox-filtering-simple-local",
    benchmark_iteration=BenchmarkIteration.BBOX_FILTERING_SIMPLE,
    cost_configuration=CostConfiguration(include_aci=True, include_blob_storage=False)
)
def _benchmark() -> None:
    min_lon = 10.40
    min_lat = 59.70
    max_lon = 10.95
    max_lat = 60.10

    gdf = gpd.read_file(
        Config.BUILDINGS_SHAPEFILE,
        bbox=(min_lon, min_lat, max_lon, max_lat),
    )
    gdf = gdf.set_crs(epsg=4326, allow_override=True).to_crs(epsg=25832)
    gdf["area"] = gdf.geometry.area
    gdf = gdf[gdf["area"] > 10]


@inject
def _download_data(
        blob_storage_service: IBlobStorageService = Provide[Containers.blob_storage_service]
) -> None:
    Config.BUILDINGS_SHAPEFILE.parent.mkdir(parents=True, exist_ok=True)

    blob_prefix = "copies/shapefile"
    base = Config.BUILDINGS_SHAPEFILE.with_suffix("")
    for ext in (".shp", ".shx", ".dbf", ".prj", ".cpg", ".qix"):
        blob_name = f"{blob_prefix}/{base.name}{ext}"
        data = blob_storage_service.download_file(
            container_name=StorageContainer.DATA,
            blob_name=blob_name,
        )
        if data is not None:
            base.with_suffix(ext).write_bytes(data)
