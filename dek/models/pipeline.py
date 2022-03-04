from dek.spark import SparkMixin
from dek.utils import LogMixin

from .context import Context, ContextTracker


class BasePipeline(LogMixin):
    """ Basic pipeline with logging util """


class SparkPipeline(BasePipeline, SparkMixin):
    """
    This base class exposes a context instance that encapsulates commonly needed objects/attributes to work with
    services such as Spark, Hive, etc.
    """

    def __init__(self, context: Context):
        _mro_cls_names = [klass.__name__ for klass in self.__class__.__mro__]
        self.logger.info(f'Inside {self.__class__.__name__}. MRO: {_mro_cls_names}')

        _namespaced_cls = self.__class__.__module__ + '.' + self.__class__.__name__
        self._context = context

        if context.spark is not None:
            self.logger.info(
                f'Initializing {_namespaced_cls} with context {self._context}. '
                'Passing SparkSession object from context.'
            )

            super().__init__(spark=self._context.spark)
        else:
            self.logger.info(
                f'Initializing {_namespaced_cls} with context {self._context}. '
                'Creating a new SparkSession object with predefined default spark configs, '
                'and assigning it to context. '
            )

            super().__init__()
            self._context.spark = self.spark
            self.logger.info(f'context instance: {self._context}.')

        ContextTracker.push_context(context=context)
