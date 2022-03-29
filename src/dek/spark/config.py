__all__ = (
    'SparkConfig',
    'DEFAULT_SPARK_CONFIG',
)


class SparkConfig:
    def __init__(
        self,
        shuffle_partitions: int = 200,
        min_executors: int = 2,
        driver_memory: str = '50g',
        driver_cores: int = 5,
        executor_memory: str = '50g',
        executor_cores: int = 5,
        executor_instances: int = 30,
    ):
        self.shuffle_partitions = shuffle_partitions
        self.min_executors = min_executors
        self.driver_memory = driver_memory
        self.driver_cores = driver_cores
        self.executor_memory = executor_memory
        self.executor_cores = executor_cores
        self.executor_instances = executor_instances


DEFAULT_SPARK_CONFIG = SparkConfig()
