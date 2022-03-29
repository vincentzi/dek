import logging
from abc import ABC

logger_ = logging.getLogger(__name__)


class cached_property:

    # When used as a decorator on instance method
    # The method name becomes an attribute name
    # The method body will execute once and ONLY once upon first attribute access

    def __init__(self, func=None, logger=None):
        self.logger = logger or logger_.info

        if func:
            self._func = func
            self.__name__ = func.__name__
            self.__module__ = func.__module__
            self.__doc__ = func.__doc__

    def __call__(self, *args, **kwargs):
        """
        It's important to return the descriptor itself to mimic built-in property object behavior
        """
        func, *_ = args

        self._func = func
        self.__name__ = func.__name__
        self.__module__ = func.__module__
        self.__doc__ = func.__doc__

        return self

    def __set__(self, instance, value):
        instance.__dict__[self.__name__] = value

    def __get__(self, instance, owner):
        if instance is None:
            return self

        value = instance.__dict__.get(self.__name__, None)

        if value is None:
            self.logger(f'Set {instance}.{self.__name__}')
            value = self._func(instance)
            instance.__dict__[self.__name__] = value

        self.logger(f'Get {instance}.{self.__name__}')

        return value


class ValidatedRuntimePropertyException(Exception):
    ...


class ValidatedRuntimeProperty(ABC):
    exception_cls: Exception = ValidatedRuntimePropertyException
    templated_err_msg = 'Attempted to retrieve a runtime property \"{name}\" which is not passed properly'

    def __init__(self, fget=None):
        self._validate()

        self.fget = fget

    def __call__(self, fget, **kwargs):
        return type(self)(self.fget, **kwargs)

    def __set_name__(self, owner, name):
        self.property_name = name

    def __get__(self, instance, owner):
        if instance is None:
            return self

        try:
            return self.fget(instance)
        except AttributeError:
            try:
                raise self.exception_cls(self.templated_err_msg.format(name=self.property_name))
            except Exception:
                raise ValidatedRuntimePropertyException(self.templated_err_msg.format(name=self.property_name))

    def _validate(self):
        if issubclass(self.exception_cls, Exception):
            return

        raise TypeError(
            f'Class attribute \"exception_class\" of class \"{self.__module__}.{self.__class__.__name__}\" '
            'must be a subclass of built-in "Exception"!!!'
        )
