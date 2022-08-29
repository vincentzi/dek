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
    from io import StringIO

    def schemaStr(struct: DataType, level: int) -> str:
        prefix = ' |   ' * level + ' |-- '
        if isinstance(struct, ArrayType):
            return f'{prefix}element: {struct.elementType.typeName()} (containsNull = {str(struct.containsNull).lower()})\n'
        elif isinstance(struct, MapType):
            return (
                f'{prefix}key: {struct.keyType.typeName()}\n'
                f'{prefix}value: {struct.valueType.typeName()} (valueContainsNull = {str(struct.valueContainsNull).lower()})\n'
            )
        elif isinstance(struct, StructField):
            return f'{prefix}{struct.name}: {struct.dataType.typeName()} (nullable = {str(struct.nullable).lower()})\n'
        else:
            return ''

    def treeStrTailRec(struct: DataType, level: int, buf: StringIO) -> None:
        if isinstance(struct, ArrayType):
            buf.write(schemaStr(struct, level))
            treeStrTailRec(struct.elementType, level + 1, buf)

        if isinstance(struct, MapType):
            buf.write(schemaStr(struct, level))
            treeStrTailRec(struct.valueType, level + 1, buf)

        if isinstance(struct, StructType):
            for field in struct.fields:
                treeStrTailRec(field, level, buf)

        if isinstance(struct, StructField):
            buf.write(schemaStr(struct, level))
            treeStrTailRec(struct.dataType, level + 1, buf)

    class _Buffer:
        def __init__(self):
            self._buffer = ''

        def write(self, content: str):
            self._buffer += content

        def getvalue(self):
            return self._buffer

    buffer = _Buffer()
    buffer.write('root\n')

    treeStrTailRec(struct=schema, level=0, buf=buffer)

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
