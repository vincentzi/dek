from typing import List, Union, Type
from pydantic import BaseModel

from dek.io import open_file
from .deserializer import FileDeserializer


class ConfLoader:
    def __init__(self, conf_dir: str):
        self.conf_dir = conf_dir

    def __call__(self, deserializer: FileDeserializer):
        self.deserializer = deserializer
        return self

    def _read_file(self, pattern: str) -> Union[dict, List[dict]]:
        file_uri = f'{self.conf_dir}/{pattern}'

        print(f'Retrieving: {file_uri}')

        file = open_file(uri=file_uri)

        return self.deserializer.read(file=file)

    def load(self, pattern: str, model: Type[BaseModel]) -> BaseModel:
        dict_obj = self._read_file(pattern)
        return self.deserializer.parse(dict_obj, model)
