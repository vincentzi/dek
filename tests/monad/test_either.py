from __future__ import annotations
from dek.monad.either import Either, Left, Right


def unsafe_parse_int(text: str) -> Left[Exception] | Right[int]:
    try:
        return Right(int(text))
    except Exception as e:
        return Left(e)


def test_either_creation():
    assert Either.pure(42) == Right(42)


def test_either_simple_chaining():
    result = Right('12s').flat_map(unsafe_parse_int)
    assert isinstance(result, Left)
    assert isinstance(result.value, Exception)

    result = Left('123').flat_map(unsafe_parse_int)
    assert repr(result) == "Left(value='123')"

    result = Left('abc').flat_map(lambda x: x + 1)
    assert result.value == 'abc'
