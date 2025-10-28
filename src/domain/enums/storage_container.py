from enum import Enum


class StorageContainer(Enum):
    METADATA = "metadata"
    SCHEMA = "schema"
    STAC = "stac"
    RAW = "raw"
    OPEN_STREET_MAP = "open_street_map"
    FKB = "fkb"
