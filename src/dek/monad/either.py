from __future__ import annotations
from typing import TypeVar, Generic, Callable
from dataclasses import dataclass

A = TypeVar('A')
B = TypeVar('B')
C = TypeVar('C')


class Either:
    @staticmethod
    def pure(x: B) -> Right[B]:
        return Right(value=x)

    def flat_map(self: 'Left[A]' | 'Right[B]', f: Callable[[B], 'Left[A]' | 'Right[C]']) -> 'Left[A]' | 'Right[C]':
        raise NotImplementedError

    def map(self: 'Left[A]' | 'Right[B]', f: Callable[[B], C]) -> 'Left[A]' | 'Right[C]':
        return self.flat_map(lambda x: type(self)(f(x)))

    def __rshift__(self, f):
        return self.flat_map(f)


@dataclass(frozen=True)
class Left(Generic[A], Either):
    value: A

    def flat_map(self: 'Left[A]' | 'Right[B]', f: Callable[[B], 'Left[A]' | 'Right[C]']) -> 'Left[A]' | 'Right[C]':
        return self


@dataclass(frozen=True)
class Right(Generic[B], Either):
    value: B

    def flat_map(self: 'Left[A]' | 'Right[B]', f: Callable[[B], 'Left[A]' | 'Right[C]']) -> 'Left[A]' | 'Right[C]':
        return f(self.value)
