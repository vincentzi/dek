import json
from typing import List, Dict, Union, IO, Type
from pydantic import BaseModel
from ruamel.yaml import YAML

from dek.models.generics import GenericFileDeserializer, GenericDocumentHandler

yaml = YAML()

# **********************************************************************************************************************
# Helper
# **********************************************************************************************************************


def parse_single_doc(obj: dict, model: Type[BaseModel]) -> BaseModel:
    """
    Given a dict object and a Pydantic model class, return a model instance
    """
    return model.parse_obj(obj)


def parse_multi_doc(obj: List[dict], model: Type[BaseModel]) -> List[BaseModel]:
    """
    Given a list of dict object and a Pydantic model class, return a list of model instances
    """
    return [model.parse_obj(s) for s in obj]

# **********************************************************************************************************************
# Interface, Base Class
# **********************************************************************************************************************


FileDeserializer = GenericFileDeserializer[BaseModel]


class DocumentHandler(GenericDocumentHandler[BaseModel]):
    ...


class DocumentDeserializer(GenericFileDeserializer[BaseModel]):
    handlers: Dict[str, DocumentHandler] = {

    }

    def __init__(self, doc_mode: str):
        self.doc_mode = doc_mode

        try:
            self.handler = self.handlers[self.doc_mode]
        except KeyError:
            raise KeyError(f'Unsupported doc mode: {self.doc_mode}')

    def read(self, file: IO) -> Union[dict, List[dict]]:
        return self.handler.reader(file)

    def parse(self, obj: Union[dict, List[dict]], model: Type[BaseModel]) -> Union[BaseModel, List[BaseModel]]:
        return self.handler.parser(obj, model)


# **********************************************************************************************************************
# Concrete Class
# **********************************************************************************************************************


class YAMLDocumentDeserializer(DocumentDeserializer):
    handlers = {
        'single': DocumentHandler(reader=yaml.load, parser=parse_single_doc),
        'multiple': DocumentHandler(reader=yaml.load_all, parser=parse_multi_doc)
    }


class JSONDocumentDeserializer(DocumentDeserializer):
    handlers = {
        'single': DocumentHandler(reader=json.load, parser=parse_single_doc),
    }


# **********************************************************************************************************************
# Actual Instance
# **********************************************************************************************************************


json_single_doc_deserializer = JSONDocumentDeserializer(doc_mode='single')
yaml_single_doc_deserializer = YAMLDocumentDeserializer(doc_mode='single')
yaml_multi_doc_deserializer = YAMLDocumentDeserializer(doc_mode='multiple')
