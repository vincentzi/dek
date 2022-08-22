from typing import Callable, TypeVar, Generic

A = TypeVar('A')
B = TypeVar('B')


class IO(Generic[A]):
    def __init__(self, run: Callable[[], A]):
        self.run = run

    def __rshift__(self, f):
        return self.flat_map(f)

    @classmethod
    def pure(cls, value: A) -> 'IO[A]':
        return cls(lambda: value)

    def flat_map(self, f: Callable[[A], 'IO[B]']) -> 'IO[B]':
        return type(self)(lambda: f(self.run()).run())

    def map(self, f: Callable[[A], B]) -> 'IO[B]':
        return type(self)(lambda: f(self.run()))
