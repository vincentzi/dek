import inspect
from functools import wraps
from pydantic import BaseModel


def parse_args(func):
    """
    Inspect function signature and try to parse argument into Pydantic model
    """

    sig = inspect.signature(func)

    parse_indicators = {}
    for annotation in sig.parameters.values():
        param = annotation.name
        type_hint = annotation.annotation
        parse_indicators[param] = type_hint

    @wraps(func)
    def _(**kwargs):
        _parsed_kwargs = {}
        for arg, val in kwargs.items():
            _type_hint = parse_indicators[arg]

            if issubclass(type(_type_hint), type(BaseModel)):
                _parsed_kwargs[arg] = _type_hint.parse_obj(val)
            else:
                _parsed_kwargs[arg] = val

        return func(**_parsed_kwargs)

    return _
