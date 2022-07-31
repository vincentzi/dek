import pytest
from pydantic import BaseModel
from dek.utils.parser import parse_args


class UploadConf(BaseModel):
    operationType: str
    fileName: str
    tableName: str


# fmt: off
@pytest.mark.parametrize(
    argnames=('obj', 'conf'),
    argvalues=[
        (object(), {'operationType': 'APPEND', 'fileName': 'test.csv', 'tableName': 'test'}),
        (object(), UploadConf(operationType='APPEND', fileName='test.csv', tableName='test')),
    ]
)
def test_parse_args(capsys, obj, conf):

    @parse_args
    def post(obj, conf: UploadConf):
        print(f'inside obj: {obj}')
        print(f'inside conf: {repr(conf)}')

        return 201, 'Accepted'

    _ = post(obj=obj, conf=conf)

    captured = capsys.readouterr()
    assert "inside obj: <object object at" in captured.out
    assert "inside conf: UploadConf(operationType='APPEND', fileName='test.csv', tableName='test')" in captured.out
# fmt: on
