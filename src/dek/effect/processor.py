from __future__ import annotations
from typing import TypeVar, Generic, Callable, Iterable, Optional
from functools import reduce

from pampy import match, _

from dek.monad.either import Either, Left, Right

E = TypeVar('E')
A = TypeVar('A')
B = TypeVar('B')


class Processor(Generic[E, A]):
    def __init__(self, valid: Iterable[A], invalid: Optional[Iterable[E]] = None):
        self._valid = tuple(valid)
        self._invalid = tuple(invalid) if invalid else tuple()

    def __repr__(self) -> str:
        return f'{type(self).__name__}(invalid={repr(self._invalid)}, valid={repr(self._valid)})'

    def __eq__(self, other: Processor) -> bool:
        if not isinstance(other, Processor):
            return False
        return self._valid == other._valid and self._invalid == other._invalid

    def __getitem__(self, item: int):
        if item == 0:
            return self._invalid
        if item == 1:
            return self._valid

        raise IndexError(
            f'A `{type(self).__name__}` obj has two items.'
            f'Index 0 or 1 is allowed but `{item}` is requested.')

    @classmethod
    def pure(cls, valid: Iterable[A]) -> 'Processor[E, A]':
        return cls(valid=valid)

    def process(self, f: Callable[[A], Either[E, B]]) -> Processor[E, B]:
        _invalid = list(self._invalid)
        _valid = []

        for item in self._valid:
            match(
                # f(item),
                # Either.pure(item).flat_map(f),
                Either.pure(item) >> f,
                Left(_), lambda e: _invalid.append(e),
                Right(_), lambda a: _valid.append(a),
            )

        return type(self)(invalid=tuple(_invalid), valid=tuple(_valid))

    def process_n(self, *fs: Callable[[A], Either[E, B]]) -> Processor[E, B]:
        _invalid = list(self._invalid)
        _valid = []

        for item in self._valid:
            match(
                reduce(lambda acc, func: acc >> func, fs, Either.pure(item)),
                Left(_), lambda e: _invalid.append(e),
                Right(_), lambda a: _valid.append(a),
            )

        return type(self)(invalid=tuple(_invalid), valid=tuple(_valid))
