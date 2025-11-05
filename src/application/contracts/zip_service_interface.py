from abc import ABC, abstractmethod


class IZipService(ABC):
    @abstractmethod
    def unzip_flat_geobuf(self, data: bytes, *layers: str) -> list[bytes]:
        """
        Unzips a flat geobuf file.
        :param data: Unzipped data.
        :param layers: Layers to extract. If left empty, all layers will be extracted.
        :return:
        """
        raise NotImplementedError
