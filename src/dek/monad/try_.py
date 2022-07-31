import traceback
from typing import Generic, TypeVar, Callable, Any, Sequence, Iterator, List
from dek.monad import Monad

__all__ = (
    'Try',
    'Success',
    'Failure',
    'TryList',
    'SuccessList',
    'FailureList',
)

T = TypeVar('T')
U = TypeVar('U')


class Try(Monad, Generic[T]):
    def __bool__(self) -> bool:
        return NotImplementedError

    @staticmethod
    def pure(
        executable: Callable[[Any], T], /, *, err_msg_callback=None
    ) -> 'Try[T]':
        try:
            return Success(executable())
        except Exception as e:
            msg = err_msg_callback() if callable(err_msg_callback) else None
            return Failure(ex=e, msg=msg)

    def flat_map(self, f: Callable[[T], 'Try[U]']) -> 'Try[U]':
        return NotImplementedError

    def map(self, f: Callable[[T], U]) -> 'Try[U]':
        return NotImplementedError

    @property
    def get(self) -> T:
        return self.val


class Success(Monad, Generic[T]):
    def __init__(self, val: T, /):
        self.val = val
        self.ex = None
        self.msg = None
        self.tb = None

    def __repr__(self) -> str:
        return f'Success({repr(self.val)})'

    def __eq__(self, other: 'Success') -> bool:
        if type(other) is Success:
            return self.val == other.val
        return False

    def __bool__(self) -> bool:
        return True

    def flat_map(self, f: Callable[[T], Try[U]]) -> Try[U]:
        return f(self.val)

    def map(self, f: Callable[[T], U]) -> Try[U]:
        return Success(f(self.val))

    @property
    def get(self) -> T:
        return self.val


class Failure(Monad, Generic[T]):
    def __init__(self, ex: Exception, tb=None, msg=None, verbose=False):
        self.val = None
        self.ex = ex
        self.msg = msg
        self.tb = tb if tb else traceback.format_exc()

        self.verbose = verbose

    def __repr__(self) -> str:
        if self.verbose:
            return f'Failure(ex={repr(self.ex)}, msg={repr(self.msg)}, tb={repr(self.tb)})'
        else:
            return f'Failure(ex={repr(self.ex)}, msg={repr(self.msg)})'

    def __bool__(self) -> bool:
        return False

    def flat_map(self, f: Callable[[T], Try[U]]) -> Try[U]:
        return self

    def map(self, f: Callable[[T], U]) -> Try[U]:
        return self


class TryList(Generic[T]):
    def __bool__(self) -> bool:
        return NotImplementedError

    def __iter__(self):
        return NotImplementedError

    def flat_map(self, f: Callable[[T], 'TryList[U]']) -> 'TryList[U]':
        return NotImplementedError

    def map(self, f: Callable[[T], U]) -> 'TryList[U]':
        return NotImplementedError

    def foreach(self, f: Callable[[T], U]):
        raise NotImplementedError

    @property
    def items(self) -> List[T]:
        return NotImplementedError

    @property
    def failures(self) -> List[Failure]:
        return NotImplementedError


class SuccessList(Monad, Generic[T]):
    def __init__(self, items: Sequence[T], /):
        self.items = list(items)
        self.failures = None

    def __repr__(self) -> str:
        return f'SuccessList({repr(self.items)})'

    def __iter__(self) -> Iterator[Success]:
        return (Success(item) for item in self.items)

    def __bool__(self) -> bool:
        return True

    def __eq__(self, other: 'SuccessList') -> bool:
        if type(other) is SuccessList:
            return self.items == other.items
        return False

    def flat_map(self, f: Callable[[T], Try[U]]) -> TryList[U]:
        successes = []
        failures = []

        for item in self.items:
            if attempt := f(item):
                successes.append(attempt)
            else:
                failures.append(attempt)

        if failures:
            return FailureList(failures)
        else:
            return SuccessList([success.get for success in successes])

    def map(self, f: Callable[[T], U]) -> TryList[U]:
        return SuccessList([f(item) for item in self.items])

    def foreach(self, f: Callable[[T], U]):
        for item in self.items:
            f(item)


class FailureList(Generic[T]):
    def __init__(self, failures: Sequence[Failure], /):
        self.items = None
        self.failures = list(failures)

    def __repr__(self) -> str:
        return f'FailureList({repr(self.failures)})'

    def __iter__(self) -> Iterator[Failure]:
        return iter(self.failures)

    def __bool__(self) -> bool:
        return False

    def flat_map(self, f: Callable[[T], Try[U]]) -> TryList[U]:
        return self

    def map(self, f: Callable[[T], U]) -> TryList[U]:
        return self

    # TODO: Do something meaningful and reasonable to signify failures
    # instead of silencing it
    def foreach(self, f: Callable[[T], U]):
        print('Failures:')
        for failure in self.failures:
            print(f'  - {repr(failure)}')
