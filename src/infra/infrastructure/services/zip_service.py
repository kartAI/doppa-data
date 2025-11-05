from io import BytesIO
from zipfile import ZipFile

from src.application.contracts import IZipService


class ZipService(IZipService):
    def unzip_flat_geobuf(self, data: bytes, *layers: str) -> list[bytes]:
        data = BytesIO(data)
        layer_bytes: list[bytes] = []
        with ZipFile(data) as zip_file:
            if len(layers) == 0:
                layer_paths = [f for f in zip_file.namelist() if f.endswith(f".fgb")]
                for layer_path in layer_paths:
                    with zip_file.open(layer_path) as f:
                        current_layer_bytes = f.read()
                        layer_bytes.append(current_layer_bytes)
            else:
                for layer in layers:
                    layer_name = f"fgb/{layer}.fgb"
                    layer_path_matches = [f for f in zip_file.namelist() if f.endswith(layer_name)]
                    if len(layer_path_matches) == 0:
                        raise ValueError(f"'{layer}' not found in the file")

                    layer_path = layer_path_matches[0]
                    with zip_file.open(layer_path) as f:
                        current_layer_bytes = f.read()
                        layer_bytes.append(current_layer_bytes)

        return layer_bytes
