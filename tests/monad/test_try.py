import pytest
from dek.monad import Try, Success, Failure, SuccessList, FailureList


def _unsafe_func(x: str) -> int:
    return int(x)


def _safe_func(x: str) -> Try[int]:
    return Try.pure(lambda: int(x))


def test_try_basics():
    successful_try = Try.pure(lambda: 1 / 1)
    failed_try = Try.pure(lambda: 1 / 0)

    assert isinstance(successful_try, Success)
    assert isinstance(failed_try, Failure)

    assert bool(successful_try) is True
    assert bool(failed_try) is False


def test_success_basics():
    assert Success('10').map(_unsafe_func) == Success(10)
    with pytest.raises(ValueError):
        Success('abc').map(_unsafe_func)

    assert Success('10').flat_map(_safe_func) == Success(10)

    result = Success('abc').flat_map(_safe_func)
    assert type(result) is Failure
    assert type(result.ex) is ValueError


def test_failure_basics():
    failure = Failure(ex=Exception('Any error'))

    assert failure.map(_unsafe_func).__class__ is Failure
    assert failure.flat_map(_safe_func).__class__ is Failure


def test_try_instance_rshift_operator():
    assert Success('10') >> _safe_func == Success(10)
    assert (
        Failure(ex=Exception('Any error')) >> _safe_func
    ).__class__ is Failure


def test_SuccessList_basics():
    success_list = SuccessList(['1', '2', '3'])
    assert repr(success_list) == "SuccessList(['1', '2', '3'])"
    assert success_list.items == ['1', '2', '3']
    assert success_list.failures is None
    assert list(iter(success_list)) == [
        Success('1'),
        Success('2'),
        Success('3'),
    ]
    assert bool(success_list) is True


def test_SuccessList_transform():
    assert SuccessList(['1', '2', '3']).flat_map(_safe_func) == SuccessList(
        [1, 2, 3]
    )
    assert SuccessList(['1', '2', '3']).flat_map(_safe_func).map(
        lambda x: x + 1
    ) == SuccessList([2, 3, 4])

    assert (
        SuccessList(['1', '2', 'abc']).flat_map(_safe_func).__class__
        is FailureList
    )
    assert (
        SuccessList(['1', '2', 'abc'])
        .flat_map(_safe_func)
        .map(lambda x: x + 1)
        .__class__
        is FailureList
    )


def test_SuccessList_side_effects(capsys):
    SuccessList(['1', '2', 'abc']).foreach(print)
    captured = capsys.readouterr()
    assert captured.out == "1\n2\nabc\n"


def test_FailureList_transform():
    failure_1 = Failure(ex=Exception('failure_1'))
    failure_2 = Failure(ex=Exception('failure_2'))

    result = FailureList([failure_1, failure_2]).flat_map(_safe_func)
    assert result.__class__ is FailureList
    assert len(result.failures) == 2

    result = (
        FailureList([failure_1, failure_2])
        .flat_map(_safe_func)
        .map(lambda x: x + 1)
    )
    assert result.__class__ is FailureList
    assert len(result.failures) == 2


def test_FailureList_side_effects(capsys):
    failure_1 = Failure(ex=Exception('failure_1'))
    failure_2 = Failure(ex=Exception('failure_2'))

    def mock_send_to_external_service(item):
        print(f'Sending {item} to external service')

    FailureList([failure_1, failure_2]).foreach(mock_send_to_external_service)
    captured = capsys.readouterr()
    assert 'failure_1' in captured.out
    assert 'failure_2' in captured.out
    assert 'Sending' not in captured.out
