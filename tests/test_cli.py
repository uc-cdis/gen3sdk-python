import pytest


def test_import_cli(monkeypatch):
    monkeypatch.setattr("sys.argv", ["gen3", "--help"])
    with pytest.raises(SystemExit) as e:
        import gen3.cli.__main__

    assert e.value.code == 0
