from pathlib import PurePosixPath

try:
    from typing import Self
except ImportError:
    from typing_extensions import Self

__all__ = (
    'ObjectStoragePath',
    'S3Path',
)

def _count_overlapping_substring(string: str, sub: str):
    count = start = 0
    while True:
        start = string.find(sub, start) + 1
        if start > 0:
            count+=1
        else:
            return count

class BadUriError(Exception):
    ...

class ObjectStoragePath:
    scheme = 'tbd'

    def __init__(self, bucket: str, key: str):
        self._bucket = bucket
        self._key = key

        self._protocol = f'{type(self).scheme}://'
        self._uri = f'{self.scheme}://{self.bucket}/{key}'
        self._namespace = f'{bucket}/{key}'

        self._key_path = PurePosixPath(key)

    def __repr__(self) -> str:
        return f'{type(self).__name__}(bucket={repr(self._bucket)}, key={repr(self._key)})'

    def __str__(self) -> str:
        return self._uri

    def __eq__(self, other) -> bool:
        if isinstance(other, type(self)):
            return self._bucket == other._bucket and self._key == other._key
        return False

    @property
    def bucket(self) -> str:
        return self._bucket

    @property
    def key(self) -> str:
        return self._key

    @property
    def uri(self) -> str:
        return self._uri

    @property
    def namespace(self) -> str:
        return self._namespace

    @property
    def suffix(self) -> str:
        return self._key_path.suffix

    @classmethod
    def is_valid_uri(cls, uri) -> bool:
        return uri.startswith(f'{cls.scheme}://') and _count_overlapping_substring(uri, '//') == 1


    @classmethod
    def from_uri(cls, uri: str) -> Self:
        if cls.is_valid_uri(uri): 
            bucket, _, key = uri.partition(f'{cls.scheme}://')[2].partition('/')
            return cls(bucket, key)

        raise BadUriError(f'Invalid uri {uri}')


class S3Path(ObjectStoragePath):
    scheme = 's3'
