from dataclasses import dataclass


@dataclass(frozen=True)
class EPSGCode:
    WGS84: int = 4326
    UTM32N: int = 25832
