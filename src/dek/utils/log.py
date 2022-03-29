import logging

__all__ = (
    'configure_console_logger',
    'LogMixin',
)


def configure_console_logger(logger=None, level=logging.INFO, propagate=True):
    if logger is None:
        print(f'{__name__}: Creating a new root logger...')
        logger = logging.getLogger()
    else:
        print(f'{__name__}: Using existing logger: {logger}')

    handler = logging.StreamHandler()
    formatter = logging.Formatter(fmt='{asctime} - {name} - {funcName} - {levelname} - {message}',
                                  datefmt='%Y-%m-%d %H:%M:%S',
                                  style='{'
                                  )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(level)
    logger.propagate = propagate


class LogMixin:
    """ Provide logger as instance attribute through mixin """

    @property
    def logger(self) -> logging.Logger:
        try:
            return self._logger
        except AttributeError:
            self._logger = logging.getLogger(f'{self.__class__.__module__}.{self.__class__.__name__}')
            return self._logger


class LogMeta(type):
    """ Provide logger as instance attribute using metaclass """
    def __new__(cls, name, bases, dct):
        instance = super().__new__(cls, name, bases, dct)
        instance.logger = logging.getLogger(f"{dct['__module__']}.{name}")
        return instance


def logutils(cls):
    """ Inject logger as instance attribute using class decorator """

    def _(*args, **kwargs):
        instance = cls(*args, **kwargs)
        instance.logger = logging.getLogger(f'{cls.__module__}.{cls.__name__}')
        return instance

    return _
