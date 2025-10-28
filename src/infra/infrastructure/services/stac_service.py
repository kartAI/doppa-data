import datetime
import json
from typing import Any

from pystac import HREF, Catalog, Collection, Item, Asset, MediaType, SpatialExtent, TemporalExtent, Extent

from src import Config
from src.application.contracts import IStacService, IBlobStorageService, IFilePathService
from src.domain.enums import StorageContainer, DataSource, Theme, BoundingBox


class StacService(IStacService):
    __blob_storage_service: IBlobStorageService
    __file_path_service: IFilePathService

    def __init__(self, blob_storage_service: IBlobStorageService, file_path_service: IFilePathService):
        self.__blob_storage_service = blob_storage_service
        self.__file_path_service = file_path_service

    def get_catalog(self, path: str) -> Catalog | None:
        json_bytes = self.__blob_storage_service.download_file(
            container_name=StorageContainer.STAC,
            blob_name="catalog.json"
        )

        if json_bytes is None:
            return None

        json_dict = json.loads(json_bytes)
        return Catalog.from_dict(d=json_dict)

    def read_text(self, source: HREF, *args: Any, **kwargs: Any) -> str:
        raise NotImplementedError

    def write_text(self, dest: HREF, txt: str, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError

    def get_catalog_root(self) -> Catalog:
        catalog = self.get_catalog("catalog.json")
        return catalog if catalog is not None else self.create_catalog(
            catalog_id="doppa-stac-root-catalog",
            title="doppa STAC Root Catalog for Norwegian Open Map Data",
            description="The root STAC catalog for doppa's open map data releases."
        )

    def create_catalog(self, catalog_id: str, description: str, title: str) -> Catalog:
        return Catalog(id=catalog_id, description=description, title=title)

    def create_collection(
            self,
            release: str,
            theme: Theme,
            start_time: datetime.datetime | None,
            bbox: tuple[float] = BoundingBox.NORWAY_WGS84.value,
            data_license: str = Config.STAC_LICENSE
    ) -> Collection:
        spatial_extent = SpatialExtent([list(bbox)])
        temporal_extent = TemporalExtent([[start_time, None]])
        extent = Extent(spatial=spatial_extent, temporal=temporal_extent)

        return Collection(
            id=theme.value,
            description=f"Collection of {theme.value} data for doppa's open map data release {release}.",
            extent=extent,
            license=data_license
        )

    def create_item(
            self,
            region: str,
            data_source: DataSource,
            geometry: dict[str, Any] | None,
            bbox: list[float] | None,
            properties: dict[str, Any],
            collection: Collection
    ) -> Item:
        asset_id = f"region-{region}-{data_source.value}"
        return Item(
            id=asset_id,
            geometry=geometry,
            bbox=bbox,
            properties=properties,
            collection=collection,
            datetime=datetime.datetime.now(datetime.UTC)
        )

    def create_asset(
            self,
            asset_path: str,
            roles: list[str] | None = ["data"],
            media_type: str | None = MediaType.PARQUET
    ) -> Asset:
        return Asset(href=asset_path, media_type=media_type, roles=roles)

    def add_asset_to_item(self, item: Item, key: str, asset: Asset) -> Item:
        item.add_asset(key=key, asset=asset)
        return item

    def add_collections_to_catalog(self, catalog: Catalog, *collections: Collection) -> Catalog:
        raise NotImplementedError

    def add_item_to_collection(self, collection: Collection, item: Item) -> Collection:
        collection.add_item(item)
        return collection

    def create_release_stac_catalog(self, root_catalog: Catalog, release: str) -> Catalog:
        release_path = self.__file_path_service.create_blob_path("release", release, "catalog.json")
        release_catalog = self.get_catalog(path=release_path)

        if release_catalog is None:
            release_catalog = self.create_catalog(
                catalog_id=release,
                title=f"doppa STAC Catalog for Release {release}",
                description=f"The STAC catalog for doppa's open map data release {release}."
            )

        release_catalog.set_self_href(href=release_path)

        root_catalog.add_child(release_catalog)
        return release_catalog
