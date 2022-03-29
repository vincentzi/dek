import io
from typing import Sequence

class InMemoryCsvFile:
    def __init__(self, field_names: Sequence[str], data: Sequence[Sequence[str]], sep: str = ','):
        header = sep.join(field_names) + '\n'
        num_fields = len(field_names)

        file = io.StringIO()
        file.write(header)

        for idx, cols in enumerate(data, 1):
            num_cols = len(cols)
            if not num_cols == num_fields:
                raise ValueError(f'# fields ({num_fields}) does not equal to # cols ({num_cols}) at row {idx}')

            try:
                row_contents = sep.join(cols)
            except TypeError:
                raise TypeError(f'cols {cols} contain values that are not string')


            file.write(row_contents + '\n')

        file.seek(0)
        self._file = file

    def __iter__(self):
        return self._file


    @classmethod
    def from_json_records(cls, records: Sequence[dict], sep: str = ',') -> 'InMemoryCsvFile':
        field_names = records[0].keys()
        data = [r.values() for r in records]
        return cls(field_names=field_names, data=data, sep=sep)

    def read(self):
        return self._file


if __name__ == '__main__':
    csv_file = InMemoryCsvFile(
        field_names=['customer_id', 'age', 'description'],
        data=[
            ['john', '20', 'hello'],
            ['mary', '40', 'world'],
        ]
    )

    # for row in csv_file.read():
    #     print(row, end='')

    # lines = csv_file.read()

    # print(repr(next(lines)))

    for line in csv_file:
        print(repr(line))
