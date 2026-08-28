"""
Test `gen3 nextflow run` (gen3.cli.nextflow, gen3.dpop_nextflow).

Every test drives the real command through CliRunner with two things stubbed: the
DPoP proxy, so nothing is exchanged for a task token or listens on a socket, and
subprocess.run, so the argv and environment Nextflow would have been given can be
read back without Nextflow being installed.
"""

from pathlib import Path
from typing import Any, Callable, Iterator
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner, Result

from gen3.auth import Gen3AuthError
from gen3.cli.nextflow import nextflow
from tests.dpop.conftest import COMMONS, TASK_TOKEN

PROXY_PORT = 61234


class _CompletedProcess:
    """Stand-in for subprocess.CompletedProcess carrying an exit code."""

    def __init__(self, returncode: int) -> None:
        self.returncode = returncode


@pytest.fixture
def fake_proxy() -> Iterator[MagicMock]:
    """Patch dpop_proxy_context so no token exchange or proxy startup happens."""
    with patch("gen3.dpop_nextflow.dpop_proxy_context") as proxy_context:
        proxy_context.return_value.__enter__.return_value = (TASK_TOKEN, PROXY_PORT)
        yield proxy_context


@pytest.fixture
def nextflow_process() -> Iterator[MagicMock]:
    """
    Patch the subprocess.run that starts Nextflow.

    A test that needs a failed run sets `return_value` or `side_effect` on the mock
    before invoking the command.
    """
    with patch("subprocess.run", return_value=_CompletedProcess(0)) as run:
        yield run


@pytest.fixture
def generated_config(nextflow_process) -> dict[str, Any]:
    """
    Capture the config `nextflow run` was given.

    The file is deleted when the run ends, so it has to be read while the child
    process is notionally running.
    """
    captured: dict[str, Any] = {}

    def capture(*args: Any, **kwargs: Any) -> _CompletedProcess:
        argv = args[0]
        captured["path"] = Path(argv[argv.index("-c") + 1])
        captured["contents"] = captured["path"].read_text()
        return _CompletedProcess(0)

    nextflow_process.side_effect = capture
    return captured


@pytest.fixture
def auth_factory() -> MagicMock:
    """
    The lazy auth factory the root group installs, naming the fake commons.

    Built per test: a factory shared between them would carry call history from
    one into the next, so nothing could be asserted about who asked for auth.
    """
    factory = MagicMock()
    factory.get.return_value.endpoint = COMMONS
    return factory


@pytest.fixture
def run_cli(
    auth_factory, fake_proxy, nextflow_process
) -> Callable[[list[str]], Result]:
    """
    Run `gen3 nextflow run` with the arguments a user would type.

    Depends on both stubs so that no test can reach a real commons or a real
    `nextflow`, whether or not it asserts on them.
    """

    def invoke(args: list[str]) -> Result:
        return CliRunner().invoke(
            nextflow, ["run"] + args, obj={"auth_factory": auth_factory}
        )

    return invoke


class TestNextflowInvocation:
    """What the Nextflow child process is given, and what its exit means."""

    def test_nextflow_args_are_forwarded(self, run_cli, nextflow_process):
        """Nextflow's own flags are appended to `nextflow run`, not rejected by Click."""
        run_cli(["main.nf", "-profile", "gen3", "-resume"])

        argv = nextflow_process.call_args.args[0]
        assert argv[:6] == [
            "nextflow",
            "run",
            "main.nf",
            "-profile",
            "gen3",
            "-resume",
        ]
        assert argv[-2] == "-c"

    def test_a_double_dash_hands_colliding_params_to_nextflow(
        self, run_cli, nextflow_process, fake_proxy
    ):
        """`--port` is a plausible pipeline param, so it has to be passable through."""
        run_cli(["main.nf", "--", "--port", "8080"])

        assert fake_proxy.call_args.kwargs["port"] is None
        assert _pipeline_args(nextflow_process) == ["main.nf", "--port", "8080"]

    def test_token_and_port_are_exported_to_nextflow(self, run_cli, nextflow_process):
        """The child environment carries the task token and the proxy port."""
        run_cli(["main.nf"])

        env = nextflow_process.call_args.kwargs["env"]
        assert env["GEN3_DPOP_BOUND_TASK_TOKEN"] == TASK_TOKEN
        assert env["GEN3_DPOP_PROXY_PORT"] == str(PROXY_PORT)

    def test_parent_environment_is_preserved(
        self, run_cli, nextflow_process, monkeypatch
    ):
        """Existing environment variables still reach Nextflow."""
        monkeypatch.setenv("NXF_HOME", "/tmp/nxf-home")

        run_cli(["main.nf"])

        assert nextflow_process.call_args.kwargs["env"]["NXF_HOME"] == "/tmp/nxf-home"

    @pytest.mark.parametrize("return_code", [0, 1, 3, 137])
    def test_the_pipeline_exit_code_becomes_the_cli_exit_code(
        self, run_cli, nextflow_process, return_code
    ):
        """Click discards return values, so a failed pipeline has to be exited on."""
        nextflow_process.return_value = _CompletedProcess(return_code)

        assert run_cli(["main.nf"]).exit_code == return_code

    def test_a_child_that_cannot_run_still_stops_the_proxy(
        self, run_cli, nextflow_process, fake_proxy
    ):
        """The proxy holds a task token, so it must not outlive a failed run."""
        nextflow_process.side_effect = OSError("boom")

        result = run_cli(["main.nf"])

        assert isinstance(result.exception, OSError)
        fake_proxy.return_value.__exit__.assert_called_once()


class TestGeneratedConfig:
    """The override that redirects a commons-facing config to the proxy."""

    def test_it_is_the_last_config_so_it_wins(self, run_cli, nextflow_process):
        """Nextflow lets the last -c win, so ours has to come after the user's."""
        run_cli(["main.nf", "-c", "mine.config"])

        argv = nextflow_process.call_args.args[0]
        assert argv[:5] == ["nextflow", "run", "main.nf", "-c", "mine.config"]
        assert argv[-2] == "-c"
        assert argv[-1].endswith("gen3-dpop.config")

    def test_it_redirects_both_endpoints_to_the_proxy(self, run_cli, generated_config):
        """The whole point: traffic aimed at the commons goes to the proxy instead."""
        run_cli(["main.nf"])

        contents = generated_config["contents"]
        proxy = f"http://127.0.0.1:{PROXY_PORT}"
        assert f"endpoint = '{proxy}/ga4gh/tes'" in contents
        assert f"endpoint = '{proxy}/s3'" in contents
        assert "s3PathStyleAccess = true" in contents

    @pytest.mark.parametrize(
        "owned_by_the_user", ["workDir", "region", "executor", "plugins"]
    )
    def test_it_overrides_nothing_else(
        self, run_cli, generated_config, owned_by_the_user
    ):
        """A setting the proxy does not force must be left to the user's config."""
        run_cli(["main.nf"])

        # The header comment names these to explain what is left alone, so only the
        # config statements themselves can be checked for them.
        statements = "\n".join(
            line
            for line in generated_config["contents"].splitlines()
            if not line.strip().startswith("//")
        )
        assert owned_by_the_user not in statements

    def test_the_upstreams_are_named_so_users_can_see_where_traffic_goes(
        self, run_cli, generated_config
    ):
        """127.0.0.1 endpoints are alarming without saying what is behind them."""
        run_cli(["main.nf"])

        assert f"{COMMONS}/ga4gh/tes" in generated_config["contents"]
        assert f"{COMMONS}/workflows/s3" in generated_config["contents"]

    def test_the_token_is_not_written_to_the_file(self, run_cli, generated_config):
        """A temp file holding a bearer-equivalent credential is world-readable."""
        run_cli(["main.nf"])

        assert TASK_TOKEN not in generated_config["contents"]
        assert "env('GEN3_DPOP_BOUND_TASK_TOKEN')" in generated_config["contents"]

    def test_the_file_is_cleaned_up_after_the_run(self, run_cli, generated_config):
        """The token lives in the environment, but the config still goes away."""
        run_cli(["main.nf"])

        assert not generated_config["path"].exists()

    def test_no_config_is_passed_when_it_is_turned_off(self, run_cli, nextflow_process):
        """--no-generated-config means the user's config is the only one."""
        run_cli(["main.nf", "--no-generated-config"])

        assert "-c" not in nextflow_process.call_args.args[0]


class TestProxyOptions:
    """The options the command owns rather than forwarding to Nextflow."""

    @pytest.mark.parametrize(
        "args,expected",
        [
            # No --port means "let the OS pick", so it arrives as None not 0.
            pytest.param(
                [],
                {
                    "port": None,
                    "task_token_type": "WORKFLOW",
                    "task_token_expiration": None,
                    "tes_endpoint": None,
                    "s3_endpoint": None,
                },
                id="defaults",
            ),
            pytest.param(["--port", "8123"], {"port": 8123}, id="port"),
            pytest.param(
                ["--task-token-type", "TASK"], {"task_token_type": "TASK"}, id="type"
            ),
            pytest.param(
                ["--task-token-expiration", "1800"],
                {"task_token_expiration": 1800},
                id="expiration",
            ),
            pytest.param(
                ["--tes-endpoint", f"{COMMONS}/ga4gh/tes"],
                {"tes_endpoint": f"{COMMONS}/ga4gh/tes"},
                id="tes_endpoint",
            ),
            pytest.param(
                ["--s3-endpoint", f"{COMMONS}/workflows/s3"],
                {"s3_endpoint": f"{COMMONS}/workflows/s3"},
                id="s3_endpoint",
            ),
        ],
    )
    def test_proxy_options_are_forwarded(
        self, run_cli, fake_proxy, nextflow_process, args, expected
    ):
        """Every proxy option reaches the proxy, and none of them reach Nextflow."""
        result = run_cli(["main.nf"] + args)

        assert result.exit_code == 0
        assert expected.items() <= fake_proxy.call_args.kwargs.items()
        assert _pipeline_args(nextflow_process) == ["main.nf"]

    def test_port_can_be_pinned_with_an_environment_variable(
        self, run_cli, fake_proxy, monkeypatch
    ):
        """The same variable the proxy exports also pins the port on the way in."""
        monkeypatch.setenv("GEN3_DPOP_PROXY_PORT", "8000")

        result = run_cli(["main.nf"])

        assert result.exit_code == 0
        assert fake_proxy.call_args.kwargs["port"] == 8000

    def test_credentials_come_from_the_root_group(
        self, run_cli, auth_factory, fake_proxy
    ):
        """Auth is resolved through the lazy factory the root group installs."""
        result = run_cli(["main.nf"])

        assert result.exit_code == 0
        auth_factory.get.assert_called_once_with()
        assert fake_proxy.call_args.kwargs["auth"] is auth_factory.get.return_value

    def test_an_endpoint_off_the_commons_fails_before_the_token_exchange(
        self, run_cli, fake_proxy, nextflow_process
    ):
        """Endpoints are resolved first, so a bad one costs no credentials."""
        result = run_cli(
            ["main.nf", "--tes-endpoint", "https://other.example.com/ga4gh/tes"]
        )

        assert result.exit_code == 1
        assert not fake_proxy.called
        assert not nextflow_process.called


class TestErrorReporting:
    """How a failed run is reported to whoever typed the command."""

    @pytest.mark.parametrize(
        "failing_fixture,error",
        [
            pytest.param(
                "fake_proxy",
                Gen3AuthError("the commons explained why"),
                id="token_exchange",
            ),
            pytest.param(
                "nextflow_process",
                FileNotFoundError("nextflow is not installed"),
                id="missing_executable",
            ),
        ],
    )
    def test_expected_failures_are_reported_without_a_traceback(
        self, request, run_cli, failing_fixture, error
    ):
        """An actionable failure exits 1 with its own message and no traceback."""
        request.getfixturevalue(failing_fixture).side_effect = error

        result = run_cli(["main.nf"])

        assert result.exit_code == 1
        assert "Traceback" not in result.output
        assert str(error) in result.output

    def test_unexpected_failures_keep_their_traceback(self, run_cli, nextflow_process):
        """A bug in the SDK is not disguised as user error."""
        nextflow_process.side_effect = ZeroDivisionError("bug")

        assert isinstance(run_cli(["main.nf"]).exception, ZeroDivisionError)

    def test_help_does_not_require_credentials(self):
        """`--help` works without any Gen3 credentials configured."""
        result = CliRunner().invoke(nextflow, ["run", "--help"])

        assert result.exit_code == 0
        assert "Usage:" in result.output


def _pipeline_args(nextflow_process: MagicMock) -> list[str]:
    """The arguments Nextflow got, without `nextflow run` or the generated `-c`."""
    argv = nextflow_process.call_args.args[0]
    return argv[2:-2]
