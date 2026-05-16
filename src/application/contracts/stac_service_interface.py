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
        Gets catalog from path. The path is resolved relative to the STAC storage container.
        :param path: Path to catalog within the STAC storage container.
        :return: Catalog or None if not found.
        :rtype: Catalog | None
        """
        raise NotImplementedError

    @abstractmethod
    def get_catalog_root(self) -> Catalog:
        """
        Get the root catalog of the STAC structure. A new one is created if none exists. The root
        catalog has its self HREF set to `{STAC_STORAGE_CONTAINER}/catalog.json`.
        :return: The root catalog.
        :rtype: Catalog
        """
        raise NotImplementedError

    @abstractmethod
    def create_catalog(self, catalog_id: str, description: str, title: str) -> Catalog:
        """
        Creates a new STAC catalog with the given identifier, description, and title. The returned
        catalog has no self HREF and no children.
        :param catalog_id: Catalog identifier.
        :param description: Catalog description.
        :param title: Catalog title.
        :return: The created catalog.
        :rtype: Catalog
        """
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
        :param release: Release on the format "YYYY-MM-DD.x".
        :param theme: Theme of the collection, e.g. Theme.BUILDINGS.
        :param start_time: Timestamp for the start of the collection's temporal extent.
        :param bbox: Bounding box for the collection's spatial extent. Formatted as
            (minX, minY, maxX, maxY). Defaults to Norway's bounding box in WGS84.
        :param data_license: License for the data in the collection, e.g. "CC-BY-4.0". Defaults to
            the value of `Config.STAC_LICENSE`.
        :return: The created collection.
        :rtype: Collection
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
        Creates an item for a specific region and data source. Items are created at theme level and
        have to be assigned to a collection. The item ID is derived as `region-{region}-{data_source}`.
        :param region: Region identifier, e.g. county code "03".
        :param data_source: Data source of the item, e.g. "osm", "fkb", "conflated".
        :param geometry: Geometry around the item. Formatted as GeoJSON specification RFC 7946,
            section 3.1.
        :param bbox: Bounding box around the item. Formatted as [minX, minY, maxX, maxY].
        :param properties: Additional properties to assign to the item.
        :param collection: The collection to which the item will be assigned.
        :return: The created item.
        :rtype: Item
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
        Creates an asset. Defaults `roles` to `["data"]` when not provided.
        :param asset_path: Path to the asset. Can be a URL or a local file path.
        :param roles: Asset roles, e.g. ["data"].
        :param media_type: Media type of the asset, e.g. MediaType.PARQUET.
        :return: Asset object.
        :rtype: Asset
        """
        raise NotImplementedError

    @abstractmethod
    def add_asset_to_item(self, item: Item, key: str, asset: Asset) -> None:
        """
        Adds an asset to an item under the given key.
        :param item: Item to add asset to.
        :type item: Item
        :param key: Asset key to use when adding the asset to the item.
        :type key: str
        :param asset: Asset to add.
        :type asset: Asset
        :return: None
        """
        raise NotImplementedError

    @abstractmethod
    def add_collections_to_catalog(self, catalog: Catalog, *collections: Collection) -> Catalog:
        """
        Adds the given collections to the catalog as children.
        :param catalog: Catalog to add the collections to.
        :param collections: Collections to add.
        :return: The catalog with the collections added.
        :rtype: Catalog
        :raises NotImplementedError: This method must be implemented by subclasses.
        """
        raise NotImplementedError

    @abstractmethod
    def add_item_to_collection(self, collection: Collection, item: Item) -> Collection:
        """
        Adds an item to a collection.
        :param collection: Collection to add the item to.
        :param item: Item to add.
        :return: The collection with the item added.
        :rtype: Collection
        """
        raise NotImplementedError

    @abstractmethod
    def create_release_catalog(self, root_catalog: Catalog, release: str) -> Catalog:
        """
        Create a STAC catalog for a specific release and assigns it to the root catalog. If a catalog
        already exists for the release, it is returned instead.
        :param root_catalog: Root STAC catalog to which the release catalog will be added.
        :param release: Release identifier on the format "YYYY-MM-DD.x".
        :return: The release catalog associated with the root catalog.
        :rtype: Catalog
        """
        raise NotImplementedError

    @abstractmethod
    def save_catalog(self, catalog: Catalog, release: str) -> None:
        """
        Saves the catalog to its self HREF location. Only the subtree under the given release is
        written; other release children are temporarily detached so they are not re-saved. The catalog
        is saved with `CatalogType.ABSOLUTE_PUBLISHED`.
        :param catalog: Catalog to save.
        :param release: Release identifier on the format "YYYY-MM-DD.x".
        :return: None
        """
        raise NotImplementedError
