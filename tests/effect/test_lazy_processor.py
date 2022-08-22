from __future__ import annotations
from dataclasses import dataclass
from dek.monad.either import Left, Right
from dek.effect.lazy_processor import LazyProcessor


def test_lazy_processor_process():
    @dataclass
    class ParsedResult:
        value: int

    def parse_int(text: str) -> Left[str] | Right[ParsedResult]:
        try:
            return Right(ParsedResult(int(text)))
        except Exception:
            return Left(f'Failed to parse `{text}`')

    p = LazyProcessor.pure(valid=iter([
        '123',
        'abc',
        '456',
        '789'
        ])
    )

    invalid, valid = p.process(parse_int)
    assert list(invalid) == [
        'Failed to parse `abc`',
    ]
    assert list(valid) == [
        ParsedResult(123),
        ParsedResult(456),
        ParsedResult(789),
    ]
