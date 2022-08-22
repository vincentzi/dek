from __future__ import annotations
from typing import TypeVar, Generic, Callable, Iterator, Optional
import itertools

from dek.monad.either import Either, Left, Right

E = TypeVar('E')
A = TypeVar('A')
B = TypeVar('B')


class LazyProcessor(Generic[E, A]):
    def __init__(self, valid: Iterator[A], invalid: Optional[Iterator[E]] = None):
        self._valid = iter(valid)
        self._invalid = iter(invalid) if invalid else iter([])

    def __repr__(self) -> str:
        return f'{type(self).__name__}(invalid={repr(self._invalid)}, valid={repr(self._valid)})'

    def __getitem__(self, item: int):
        if item == 0:
            return self._invalid
        if item == 1:
            return self._valid

        raise IndexError(
            f'A `{type(self).__name__}` obj has two items.'
            f'Index 0 or 1 is allowed but `{item}` is requested.')

    @classmethod
    def pure(cls, valid: Iterator[A]) -> 'LazyProcessor[E, A]':
        return cls(valid=valid)

    def process(self, f: Callable[[A], Either[E, B]]) -> LazyProcessor[E, B]:
        _invalid = self._invalid
        _valid = iter([])

        for item in self._valid:
            result = Either.pure(item) >> f

            print(f'{result=!r}')

            if isinstance(result, Left):
                _invalid = itertools.chain(_invalid, [result.value])
                continue

            if isinstance(result, Right):
                _valid = itertools.chain(_valid, [result.value])
            else:
                raise TypeError(f'Incompatible type: {type(item)} for item: {item}')

        return type(self)(valid=_valid, invalid=_invalid)
