from __future__ import annotations
from dataclasses import dataclass

from dek.monad.either import Left, Right
from dek.effect.processor import Processor


def test_processor_creation():
    valid = ['product1', 'product2', 'product3']
    invalid = [Exception('Invalid product code `abc`'), Exception('Invalid product code `def`')]

    p1 = Processor(valid=valid, invalid=invalid)
    p2 = Processor.pure(valid=valid)
    p3 = Processor(valid=valid)

    assert p2 == p3

    _, valid_1 = p1
    _, valid_2 = p2
    _, valid_3 = p3

    assert valid_1 == valid_2 == valid_3


def test_processor_process():
    @dataclass
    class ParsedResult:
        value: int

    def parse_int(text: str) -> Left[str] | Right[ParsedResult]:
        try:
            return Right(ParsedResult(int(text)))
        except Exception:
            return Left(f'Failed to parse `{text}`')

    def extract_even_num(num: ParsedResult) -> Left[str] | Right[int]:
        unwrapped = num.value

        if unwrapped % 2 == 0:
            return Right(unwrapped)
        else:
            return Left(f'{repr(num)} is not even')

    p = Processor.pure(valid=[
        '123',
        'abc',
        '456',
        '789'
    ])

    invalid, valid = p.process(parse_int)
    assert p.process(parse_int) == p.process_n(parse_int)
    assert invalid == (
        'Failed to parse `abc`',
    )
    assert valid == (
        ParsedResult(123),
        ParsedResult(456),
        ParsedResult(789),
    )

    invalid, valid = p.process_n(parse_int, extract_even_num)

    print(f'{invalid=!r}')
    assert invalid == (
        f'{repr(ParsedResult(123))} is not even',
        'Failed to parse `abc`',
        f'{repr(ParsedResult(789))} is not even',
    )
    assert valid == (
        456,
    )


