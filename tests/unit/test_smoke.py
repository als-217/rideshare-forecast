import rideforecast


def test_package_imports():
    assert hasattr(rideforecast, "main")


def test_main_prints_greeting(capsys):
    rideforecast.main()
    captured = capsys.readouterr()
    assert "rideforecast" in captured.out
