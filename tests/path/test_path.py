import pytest
from dek.path.paths import S3Path, ObjectStoragePath

def test_s3path_good_uri():
    path = S3Path.from_uri('s3://mybucket/folder/subfolder/sample.txt.gzip')

    assert str(path) == 's3://mybucket/folder/subfolder/sample.txt.gzip'
    assert repr(path) == "S3Path(bucket='mybucket', key='folder/subfolder/sample.txt.gzip')"

    assert path.bucket == 'mybucket'
    assert path.key == 'folder/subfolder/sample.txt.gzip'
    assert path.uri == 's3://mybucket/folder/subfolder/sample.txt.gzip'
    assert path.scheme == 's3'
    assert path.namespace == 'mybucket/folder/subfolder/sample.txt.gzip'

    assert isinstance(path, ObjectStoragePath)

@pytest.mark.parametrize(
    'uri,is_valid',
    [
        ('s3:///mybucket/folder/subfolder/sample.txt.gzip', False),
        ('s3://mybucket//folder/subfolder/sample.txt.gzip', False),
        ('s3://mybucket///folder/subfolder/sample.txt.gzip', False),

        ('/mybucket/folder/subfolder/sample.txt.gzip', False),
        ('mybucket/folder/subfolder/sample.txt.gzip', False),
    ]
)
def test_s3path_bad_uris(uri, is_valid):
    assert S3Path.is_valid_uri(uri) == is_valid

def test_s3path_equality():
    path1 = S3Path.from_uri('s3://mybucket/folder/subfolder/sample.txt.gzip')
    path2 = S3Path(bucket='mybucket', key='folder/subfolder/sample.txt.gzip')

    assert path1 == path2
