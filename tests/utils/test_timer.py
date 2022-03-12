from time import sleep

from freezegun import freeze_time
from dek.utils.timer import Timer

@freeze_time('2022-01-01 00:00:10', auto_tick_seconds=1)
def test_timer_context_manager(capsys):
    with Timer(text='time: {:0.2f} seconds', log=print):
        sleep(3)

    captured = capsys.readouterr()
    assert captured.out == "time: 1.00 seconds\n"


