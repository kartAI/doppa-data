from enum import Enum


class DataSource(Enum):
    OSM = "osm"
    FKB = "fkb"
    CONFLATED = "conflated"
