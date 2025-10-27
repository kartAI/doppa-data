from abc import ABC

import pystac


class IStacService(pystac.StacIO, ABC):
    pass
