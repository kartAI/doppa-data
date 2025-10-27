from typing import Any

from pystac import HREF

from src.application.contracts import IStacService


class StacService(IStacService):
    def read_text(self, source: HREF, *args: Any, **kwargs: Any) -> str:
        pass

    def write_text(self, dest: HREF, txt: str, *args: Any, **kwargs: Any) -> None:
        pass
