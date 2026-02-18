from enum import Enum


class StorageContainer(Enum):
    CONTRIBUTION = "contributions"
    METADATA = "metadata"
    SCHEMA = "schema"
    STAC = "stac"
    RAW = "raw"
    DATA = "data"
    OPEN_STREET_MAP = "open_street_map"
    FKB = "fkb"
    BENCHMARKS = "benchmarks"
