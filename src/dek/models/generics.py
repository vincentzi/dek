from typing import Optional, List, TypeVar, Generic, IO, Union, Type, Callable

T = TypeVar('T')
MODEL = TypeVar('MODEL')


class GenericContextTracker(Generic[T]):
    """
    A generic class to store, track, and dispatch scoped context/state objects
    """

    _current_managed_context: Optional[T] = None
    _previous_managed_contexts: List[T] = []

    @classmethod
    def push_context(cls, context: T):
        if cls._current_managed_context:
            cls._previous_managed_contexts.append(cls._current_managed_context)
        cls._current_managed_context = context

    @classmethod
    def pop_context(cls) -> Optional[T]:
        current_context = cls._current_managed_context

        if cls._previous_managed_contexts:
            cls._current_managed_context = cls._previous_managed_contexts.pop()
        else:
            cls._current_managed_context = None

        return current_context

    @classmethod
    def get_current_context(cls) -> Optional[T]:
        return cls._current_managed_context


# **********************************************************************************************************************
# File Parsing
# **********************************************************************************************************************


class GenericFileDeserializer(Generic[MODEL]):
    def read(self, file: IO) -> Union[dict, List[dict]]:
        """
        Convert a file-like object into a dict or a list of dict
        """
        raise NotImplementedError

    def parse(
        self, obj: Union[dict, List[dict]], model: Type[MODEL]
    ) -> Union[MODEL, List[MODEL]]:
        """
        Given a dict or a list of dict and a model class, return a model instance or a list of model instances
        """
        raise NotImplementedError


class GenericDocumentHandler(Generic[MODEL]):
    def __init__(
        self,
        reader: Callable[[IO], Union[dict, List[dict]]],
        parser: Union[
            Callable[[dict, Type[MODEL]], MODEL],
            Callable[[List[dict], Type[MODEL]], List[MODEL]],
        ],
    ):
        self.reader = reader
        self.parser = parser
