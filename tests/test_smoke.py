from pert_gym import __version__
from pert_gym.cli import main


def test_version() -> None:
    assert __version__ == "0.1.0"


def test_cli_info() -> None:
    assert main(["info"]) == 0
