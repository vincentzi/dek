import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    ArrayType,
    MapType,
    DataType,
)

logger = logging.getLogger(__name__)


def _treeString(schema: StructType) -> str:
    # helpers
    from io import StringIO

    def prefix(level: int) -> str:
        head = ' '
        tail = '--'
        body = '|'
        _4_SPACES = ' ' * 4

        for _ in range(level - 1):
            body += f'{_4_SPACES}|'

        return head + body + tail

    def process(struct: DataType, level: int, buf: StringIO):
        if isinstance(struct, StructType):
            for field in struct.fields:
                process(field, level, buf)

        elif isinstance(struct, ArrayType):
            buf.write(
                f'{prefix(level)} element: {struct.elementType.typeName()} (containsNull = {str(struct.containsNull).lower()})\n'
            )
            process(struct.elementType, level + 1, buf)

        elif isinstance(struct, MapType):
            buf.write(f'{prefix(level)} key: {struct.keyType.typeName()}\n')
            buf.write(
                f'{prefix(level)} value: {struct.valueType.typeName()} (valueContainsNull = {str(struct.valueContainsNull).lower()})\n'
            )
            process(struct.valueType, level + 1, buf)

        elif isinstance(struct, StructField):
            buf.write(
                f'{prefix(level)} {struct.name}: {struct.dataType.typeName()} (nullable = {str(struct.nullable).lower()})\n'
            )

            if isinstance(struct.dataType, ArrayType):
                process(struct.dataType, level + 1, buf)
            if isinstance(struct.dataType, StructType):
                process(struct.dataType, level + 1, buf)

    # main
    class _Buffer:
        def __init__(self):
            self._buffer = ''

        def write(self, content: str):
            self._buffer += content

        def getvalue(self):
            return self._buffer

    # buffer = StringIO(newline=None)
    buffer = _Buffer()
    buffer.write('root\n')
    process(struct=schema, level=1, buf=buffer)

    return buffer.getvalue()


def patch_StructType():
    StructType.treeString = lambda self: _treeString(self)


def patch_SparkSession():
    def LogArg(func):
        def _(self, sqlQuery):
            logger.info(f'Executing Spark SQL\n{sqlQuery}')
            return func(self, sqlQuery)

        return _

    SparkSession.sql = LogArg(SparkSession.sql)
