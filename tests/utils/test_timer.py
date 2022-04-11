from time import sleep

from freezegun import freeze_time
from testfixtures import log_capture
from dek.utils.timer import Timer


@freeze_time('2022-01-01 00:00:10', auto_tick_seconds=1)
def test_timer_context_manager(capsys):
    with Timer(text='time: {:0.2f} seconds', log=print):
        sleep(1)

    captured = capsys.readouterr()
    assert captured.out == "time: 1.00 seconds\n"


@freeze_time('2022-01-01 00:00:10', auto_tick_seconds=1)
def test_timer_decorates_function_with_params(capsys):
    @Timer(text='time: {:0.2f} seconds', log=print)
    def some_function():
        sleep(1)

    some_function()

    captured = capsys.readouterr()
    assert captured.out == "time: 1.00 seconds\n"


@freeze_time('2022-01-01 00:00:10', auto_tick_seconds=1)
@log_capture()
def test_timer_decorates_function_without_parens(capture):
    @Timer
    def some_function():
        sleep(1)

    some_function()

    capture.check(
        ('dek.utils.timer.Timer', 'INFO', 'Elapsed time: 1.0000 seconds'),
    )


@freeze_time('2022-01-01 00:00:10', auto_tick_seconds=1)
@log_capture()
def test_timer_decorates_function_with_parens_no_params(capture):
    @Timer()
    def some_function():
        sleep(1)

    some_function()

    capture.check(
        ('dek.utils.timer.Timer', 'INFO', 'Elapsed time: 1.0000 seconds'),
    )


@freeze_time('2022-01-01 00:00:10', auto_tick_seconds=1)
@log_capture()
def test_timer_decorates_instance_method_without_parens(capture):
    class SomeClass:
        @Timer
        def some_method(self):
            sleep(1)

    instance = SomeClass()
    instance.some_method()

    capture.check(
        ('dek.utils.timer.Timer', 'INFO', 'Elapsed time: 1.0000 seconds'),
    )


@freeze_time('2022-01-01 00:00:10', auto_tick_seconds=1)
@log_capture()
def test_timer_decorates_class_without_parens(capture):
    @Timer
    class SomeClass:
        def __init__(self):
            sleep(1)

    _ = SomeClass()

    capture.check(
        ('dek.utils.timer.Timer', 'INFO', 'Elapsed time: 1.0000 seconds'),
    )
