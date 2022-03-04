import logging
from typing import Union, IO

import smart_open

logger = logging.getLogger(__name__)


def open_file(uri: Union[str, object], mode='r', encoding=None) -> IO:
    logger.info(f'Reading file {uri}')

    return smart_open.open(uri=uri, mode=mode, encoding=encoding,)
