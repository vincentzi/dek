from apache_beam.transforms.core import DoFn, CallableWrapperDoFn


@CallableWrapperDoFn
def text_split(text: str, sep: str = ','):
    for word in text.split(sep):
        yield word


class SplitWordsDoFn(DoFn):
    def __init__(self, sep: str):
        self.sep = sep

    def process(self, text: str):
        for word in text.split(sep=self.sep):
            yield word
