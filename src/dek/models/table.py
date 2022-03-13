from abc import ABC


class Table(ABC):
    table_name: str
    column_def: str

    @property
    def resolve_table_name(self) -> str:
        return self.table_name


class PartitionedTable(Table):
    partition_def: dict


class WeekSuffixTable(Table):
    """
    Class for keeping track of a table with week identifier
    """
    def __call__(self, week_identifier: str) -> 'WeekSuffixTable':
        self.week_identifier = week_identifier
        return self

    @property
    def resolve_table_name(self) -> str:
        return self.table_name.format(self.week_identifier)
