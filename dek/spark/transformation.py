from types import MethodType
from typing import List, Iterable, Callable, TypeVar
from functools import reduce, partial

from pyspark.sql import DataFrame

__all__ = (
    'union_dfs',
    'repeated_transform',
)

T = TypeVar('T')


def union_dfs(dfs: List[DataFrame]) -> DataFrame:
    return reduce(lambda df1, df2: df1.unionByName(df2), dfs)


def repeated(input_df: DataFrame, units: Iterable[T], unit_of_work: Callable[[T], DataFrame], **kwargs) -> DataFrame:
    return reduce(
        lambda df, transform: MethodType(transform, df)(),
        [unit_of_work(u) for u in units],
        input_df
    )


def repeated_transform(
        units: Iterable[T],
        unit_of_work: Callable[[T], Callable[[DataFrame], DataFrame]]
) -> Callable[[DataFrame], DataFrame]:
    return partial(repeated, units=units, unit_of_work=unit_of_work)
