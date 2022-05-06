from apache_beam import Create, ParDo
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from dek.beam.example import SplitWordsDoFn, text_split


def test_SplitWordsDoFn():
    with TestPipeline() as p:
        people = (
            p
            | 'Read names' >> Create(['vincent,max,eric', 'nancy,lily'])
            | 'Split words'
            >> ParDo(SplitWordsDoFn(sep=',')).with_output_types(str)
        )

        assert_that(
            people, equal_to(['vincent', 'max', 'eric', 'nancy', 'lily'])
        )


def test_text_split():
    with TestPipeline() as p:
        people = (
            p
            | 'Read names' >> Create(['vincent,max,eric', 'nancy,lily'])
            | 'Split words' >> ParDo(text_split).with_output_types(str)
        )

        assert_that(
            people, equal_to(['vincent', 'max', 'eric', 'nancy', 'lily'])
        )
