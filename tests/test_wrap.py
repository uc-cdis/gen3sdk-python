import pytest
from unittest.mock import MagicMock, patch

from gen3.tools.wrap import Gen3Wrap, Gen3Auth, Gen3AuthError


# Patch subprocess.run and ensure appropriate args are sent to the method
@patch.object(Gen3Auth, "get_access_token", MagicMock(return_value="1.2.3"))
def test_gen3_wrap_valid_auth(mock_gen3_auth):
    test_command_args = ("echo", "Test1", "Test2")
    with patch("gen3.tools.wrap.subprocess.run") as mock_subprocess_run:
        wrapper_obj = Gen3Wrap(mock_gen3_auth, test_command_args)
        wrapper_obj.run_command()
        mock_subprocess_run.assert_called_once_with(list(test_command_args), stderr=-2)


# Try to break the auth and see if Gen3AuthError is thrown
@patch.object(Gen3Auth, "get_access_token", MagicMock(side_effect=Gen3AuthError()))
def test_gen3_wrap_inavalid_auth(mock_gen3_auth):
    test_command_args = ("echo", "Test1", "Test2")
    with pytest.raises(Gen3AuthError):
        with patch("gen3.tools.wrap.subprocess.run") as _:
            wrapper_obj = Gen3Wrap(mock_gen3_auth, test_command_args)
            wrapper_obj.run_command()
