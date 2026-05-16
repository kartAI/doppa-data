import geopandas as gpd
from dependency_injector.wiring import inject, Provide

from src import Config
from src.application.common.monitor import monitor
from src.application.contracts import IBlobStorageService
from src.application.dtos import CostConfiguration
from src.domain.enums import StorageContainer, BenchmarkIteration, BoundingBox
from src.infra.infrastructure import Containers


def bbox_filtering_result_set_sizes_neighborhood_local() -> None:
    """
    Benchmark: bounding-box filter at neighborhood scale over the buildings
    shapefile, executed locally via GeoPandas. Downloads the pre-baked shapefile
    copy from blob storage before running the timed area-filter pipeline.
    """
    _download_data()
    _benchmark()


@monitor(
    query_id="bbox-filtering-result-set-sizes-neighborhood-local",
    benchmark_iteration=BenchmarkIteration.BBOX_FILTERING_RESULT_SET_SIZES,
    cost_configuration=CostConfiguration(include_aci=True, include_blob_storage=False)
)
def _benchmark() -> None:
    min_lon, min_lat, max_lon, max_lat = BoundingBox.NEIGHBORHOOD_WGS84.value

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
