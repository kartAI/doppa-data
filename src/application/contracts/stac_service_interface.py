import datetime
from abc import ABC, abstractmethod
from typing import Any

from pystac import Catalog, Collection, Item, Asset, MediaType

from src import Config
from src.domain.enums import DataSource, Theme, BoundingBox


class IStacService(ABC):
    @abstractmethod
    def get_catalog(self, path: str) -> Catalog | None:
        """
        Gets catalog from path
        :param path: Path to catalog
        :return: Catalog or None if not found
        :rtype: Catalog | None
        """
        raise NotImplementedError

    @abstractmethod
    def get_catalog_root(self) -> Catalog:
        """
        Get the root catalog of the STAC structure. A new one is created if none exists.
        :return: The root catalog.
        :rtype: Catalog
        """
        raise NotImplementedError

    @abstractmethod
    def create_catalog(self, catalog_id: str, description: str, title: str) -> Catalog:
        raise NotImplementedError

    @abstractmethod
    def create_collection(
            self,
            release: str,
            theme: Theme,
            start_time: datetime.datetime | None,
            bbox: tuple[float] = BoundingBox.NORWAY_WGS84.value,
            data_license: str = Config.STAC_LICENSE
    ) -> Collection:
        """
        Creates a collection for a specific release and theme.
        :param release: Release on the format "YYYY-MM-DD.x"
        :param theme: Theme of the collection, e.g. Theme.BUILDINGS
        :param start_time: Timestamp for the start of the collection's temporal extent.
        :param bbox: Bounding box for the collection's spatial extent. Formatted as (minX, minY, maxX, maxY). Defaults to Norway's bounding box in WGS84.
        :param data_license: License for the data in the collection, e.g. "CC-BY-4.0". Defaults to CC-BY-4.0.
        :return:
        """
        raise NotImplementedError

    @abstractmethod
    def create_item(
            self,
            region: str,
            data_source: DataSource,
            geometry: dict[str, Any] | None,
            bbox: list[float] | None,
            properties: dict[str, Any],
            collection: Collection
    ) -> Item:
        """
        Creates an item for a specific region and data source. Items are created at theme level and have to be assigned to a collection.
        :param region: Region identifier, e.g. county code "03"
        :param data_source: Data source of the item, e.g. "osm", "fkb", "conflated"
        :param geometry: Geometry around the item. Formatted as GeoJSON specification RFC 7946, section 3.1
        :param bbox: Bounding box around the item. Formatted as [minX, minY, maxX, maxY]
        :param properties: Additional properties to assign to the item
        :param collection: The collection to which the item will be assigned
        :return: Returns the created item
        """
        raise NotImplementedError

    @abstractmethod
    def create_asset(
            self,
            asset_path: str,
            roles: list[str] | None = None,
            media_type: str | None = MediaType.PARQUET
    ) -> Asset:
        """
        Creates an asset
        :param asset_path: Path to the asset. Can be a URL or a local file path
        :param roles: Asset roles, e.g. ["data"]
        :param media_type: Media type of the asset, e.g. MediaType.PARQUET
        :return: Asset object
        """
        raise NotImplementedError

    @abstractmethod
    def add_asset_to_item(self, item: Item, key: str, asset: Asset) -> None:
        """
        Adds an asset to an item
        :param key: Asset key to use when adding the asset to the item
        :param item: Item to add asset to
        :type item: Item
        :param asset: Asset to add
        :type asset: Asset
        :return: Input item with added asset
        :rtype: Item
        """
        raise NotImplementedError

    @abstractmethod
    def add_collections_to_catalog(self, catalog: Catalog, *collections: Collection) -> Catalog:
        raise NotImplementedError

    @abstractmethod
    def add_item_to_collection(self, collection: Collection, item: Item) -> Collection:
        raise NotImplementedError

    @abstractmethod
    def create_release_catalog(self, root_catalog: Catalog, release: str) -> Catalog:
        """
        Create a STAC catalog for a specific release and assigns it to the root catalog. If a catalog already exists for the release, it is returned instead.
        :param root_catalog: Root STAC catalog to which the release catalog will be added.
        :param release: Release identifier on the format "YYYY-MM-DD.x"
        :return: The release catalog associated with the root catalog.
        :rtype: Catalog
        """
        raise NotImplementedError

    @abstractmethod
    def save_catalog(self, catalog: Catalog, release: str) -> None:
        """
        Saves catalog to its self href location
        :param catalog:
        :param release:
        :return:
        """
        raise NotImplementedError
