# Python 2 and 3 compatible
try:
    from unittest.mock import patch
except ImportError:
    from mock import patch


def test_import_index():
    """
    Try to import the index package.
    """
    import gen3.indexclient


@patch("gen3.indexclient.client.handle_error")
@patch("requests.get")
def test_hashes(get_request_mock, handle_error_mock):
    from gen3.indexclient.client import IndexClient

    input_params = {"hashes": {"md5": "00000000000000000000000000000001"}, "size": "1"}

    expected_format = {
        "hash": ["md5:00000000000000000000000000000001"],
        "size": "1",
        "limit": 1,
    }

    with patch("gen3.indexclient.client.IndexClient._get") as get_mock:
        client = IndexClient("base_url")
        client.get_with_params(input_params)

        assert get_mock.called
        args, kwargs = get_mock.call_args_list[0]
        assert kwargs["params"] == expected_format
