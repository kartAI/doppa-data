from abc import ABC

from pystac.stac_io import DefaultStacIO


class IStacIOService(DefaultStacIO, ABC):
    @property
    def skip_file_download(self) -> bool:
        raise NotImplementedError

    @skip_file_download.setter
    def skip_file_download(self, value: bool) -> None:
        raise NotImplementedError

    def strip_path_stem(self, path: str) -> str:
        """
        Removes the leading part of the path up to and including 'stac/'. Only intended for use with STAC HREFs with 'stac/' in them.
        :param path: Input path
        :return: Storage container compatible path
        """
        raise NotImplementedError
