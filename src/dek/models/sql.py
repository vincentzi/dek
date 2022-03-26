import io
from typing import Mapping, Callable, Any, Union
from abc import ABC, abstractmethod
import pandas as pd

from dek.fs import open_file
from dek.utils import LogMixin

__all__ = (
    'SQLScriptStore',
    'SQLExecutor',
)


class SQLExecutor(ABC):

    @abstractmethod
    def execute(self): ...

    @abstractmethod
    def fetch_df(self) -> pd.DataFrame: ...

    def fetch_result(self, callback: Callable[[pd.DataFrame], Any]):
        callback(self.fetch_df())


class SQLScriptStore(Mapping, LogMixin):
    """
    A custom mapping class that lookup and build HQL instance from script
    """
    _scripts = {}

    def __init__(self, root_dir: str, params: dict, executor: SQLExecutor = None):
        self.root_dir = root_dir
        self.params = params
        self.executor = executor

    def __getitem__(self, key) -> Union[str, SQLExecutor]:
        if self.executor is None:
            return self._get_sql(key)
        else:
            return self.executor(sql=self._get_sql(key))

    def __iter__(self):
        return iter(self._scripts)

    def __len__(self):
        return len(self._scripts)

    @staticmethod
    def _line_has_comment(line: str) -> bool:
        return line.strip().startswith('--')

    def _get_sql(self, script_name) -> str:
        script_uri = f'{self.root_dir}/{script_name}.sql'

        try:
            _sql = self._scripts[script_name]
            self.logger.info(f'Retrieve existing SQL from script store')
            return _sql
        except KeyError:
            self.logger.info(f'Creating new SQL from {script_uri}')
            file_stream = io.StringIO()

            with open_file(script_uri, 'r') as f:
                for line in f:
                    if not self._line_has_comment(line):
                        file_stream.write(line)

                sql_str = file_stream.getvalue().format(**self.params).strip()

                return sql_str
