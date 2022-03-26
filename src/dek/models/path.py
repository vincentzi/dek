from pathlib import PurePosixPath

__all__ = (
    'ObjectStoragePath',
    'S3Path',
)

def _count_overlapping_substring(string: str, sub: str) -> int:
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

    uri_qualifiers = [
        lambda uri: uri.endswith('.') == False,
        lambda uri: _count_overlapping_substring(uri, '//') == 1,
    ]

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
    def is_qualified_uri(cls, uri) -> bool:
        _qualifiers = [
            lambda uri: uri.startswith(f'{cls.scheme}://'), 
            *cls.uri_qualifiers,
        ]

        return all([q(uri) for q in _qualifiers])

    @classmethod
    def from_uri(cls, uri: str) -> 'ObjectStoragePath':
        if cls.is_qualified_uri(uri): 
            bucket, _, key = uri.partition(f'{cls.scheme}://')[2].partition('/')
            return cls(bucket, key)

        raise BadUriError(f'Invalid uri {uri}')


class S3Path(ObjectStoragePath):
    scheme = 's3'
