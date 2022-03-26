import pytest
from dek.repository import InMemoryCsvFile


def test_InMemoryCsvFile_init():
    csv_file = InMemoryCsvFile(
        field_names=['customer_id', 'age', 'description'],
        data=[
            ['john', '20', 'hello'],
            ['mary', '40', 'world'],
        ]
    )

    lines = iter(csv_file.read())

    assert next(lines) == 'customer_id,age,description\n'
    assert next(lines) == 'john,20,hello\n'
    assert next(lines) == 'mary,40,world\n'


def test_InMemoryCsvFile_bad_row_content():
    with pytest.raises(TypeError):
        _ = InMemoryCsvFile(
            field_names=['customer_id', 'age', 'description'],
            data=[
                ['john', '20', 'hello'],
                ['mary', 40, 'world'],
            ]
        )


def test_InMemoryCsvFile_header_row_mismatch():
    with pytest.raises(ValueError):
        _ = InMemoryCsvFile(
            field_names=['customer_id', 'age', 'description'],
            data=[
                ['john', '20'],
                ['mary', '40'],
            ]
        )


def test_InMemoryCsvFile_from_dict_records():
    dict_records = [
        {'customer_id': 'john', 'age': '20', 'description': 'hello'},
        {'customer_id': 'mary', 'age': '40', 'description': 'world'},
    ]

    csv_file = InMemoryCsvFile.from_json_records(records=dict_records)

    lines = iter(csv_file.read())

    assert next(lines) == 'customer_id,age,description\n'
    assert next(lines) == 'john,20,hello\n'
    assert next(lines) == 'mary,40,world\n'

