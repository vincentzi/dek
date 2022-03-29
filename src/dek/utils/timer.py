import logging
import time
from functools import wraps
from types import MethodType

__all__ = ('Timer',)


class TimerError(Exception):
    """A custom exception used to report errors in use of Timer class"""


class Timer:
    """
    A timing utility class that can be used as decorator or context manager

    """

    def __init__(
        self, func=None, text="Elapsed time: {:0.4f} seconds", log=None
    ):
        self._start_time = None
        self.text = text

        _logger = logging.getLogger(
            self.__class__.__module__ + '.' + self.__class__.__name__
        )
        self.log = log or _logger.info

        if func:
            self._func = func
            wraps(self._func)(self)

    def __enter__(self):
        """Start a new timer as a context manager"""
        self.start()
        return self

    def __exit__(self, *exc_info):
        """Stop the context manager timer"""
        self.stop()

    def __call__(self, *args, **kwargs):
        """Support using Timer as a decorator"""
        if hasattr(self, '_func'):
            with self:
                result = self.__wrapped__(*args, **kwargs)

            return result
        else:
            func, *_ = args

            @wraps(func)
            def wrapper_timer(*args1, **kwargs1):
                with self:
                    return func(*args1, **kwargs1)

            return wrapper_timer

    def __get__(self, instance, owner):
        """Descriptor needed to make decorator work with instance method"""
        if instance is None:
            return self
        else:
            return MethodType(self, instance)

    def start(self):
        """Start a new timer"""
        if self._start_time is not None:
            raise TimerError("Timer is running. Use .stop() to stop it")

        self._start_time = time.perf_counter()

    def stop(self):
        """Stop the timer, and report the elapsed time"""
        if self._start_time is None:
            raise TimerError("Timer is not running. Use .start() to start it")

        elapsed_time = time.perf_counter() - self._start_time
        self._start_time = None

        self.log(self.text.format(elapsed_time))

        return elapsed_time
