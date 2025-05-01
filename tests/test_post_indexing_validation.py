import pytest
import os
from gen3.tools.indexing.post_indexing_validation import (
    validate_manifest,
    ManifestError,
)
from unittest import mock
from unittest.mock import MagicMock, mock_open
import gen3
import time
import base64
from requests import HTTPError

cwd = os.path.dirname(os.path.realpath(__file__))


read_data = "guid,md5,size,acl,url\n255e396f-f1f8-11e9-9a07-0a80fada099c,473d83400bc1bc9dc635e334faddf33d,363455714,[Open],[s3://pdcdatastore/test1.raw]\n255e396f-f1f8-11e9-9a07-0a80fada098d,473d83400bc1bc9dc635e334faddd33c,343434344,[Open],[s3://pdcdatastore/test2.raw]\n255e396f-f1f8-11e9-9a07-0a80fada097c,473d83400bc1bc9dc635e334fadd433c,543434443,[phs0001],[s3://pdcdatastore/test3.raw]"
read_data2 = "guid,md5,size,acl,url\n255e396f-f1f8-11e9-9a07-0a80fada099c,473d83400bc1bc9dc635e334faddf33d,363455714,[Open],[s3://pdcdatastore/test1.raw]\n255e396f-f1f8-11e9-9a07-0a80fada098d,473d83400bc1bc9dc635e334faddd33c,343434344,[Open],[s3://pdcdatastore/test2.raw]\n255e396f-f1f8-11e9-9a07-0a80fada097c,473d83400bc1bc9dc635e334fadd433c,543434443,[phs0001],[invalid_format]"


class MockResponse:
    def __init__(self, status, ok=True, json={"url": "my_presigned_url"}):
        self.status_code = status
        self.ok = ok
        self.json_data = json

    def raise_for_status(response):
        pass

    def text(self):
        return self._text

    def read(self):
        pass

    def json(self):
        return self.json_data


def test_validate_manifest_coro_with_200():
    hostname = "test.datacommons.io"
    exp = time.time() + 300
    decoded_info = {"aud": "123", "exp": exp, "iss": f"http://{hostname}"}
    test_key = {
        "api_key": "whatever."  # pragma: allowlist secret
        + base64.urlsafe_b64encode(
            ('{"iss": "http://%s", "exp": %d }' % (hostname, exp)).encode("utf-8")
        ).decode("utf-8")
        + ".whatever"
    }
    get_mock = MockResponse(200)
    with mock.patch(
        "gen3.auth.get_access_token_with_key"
    ) as mock_access_token, mock.patch(
        "gen3.auth.Gen3Auth._write_to_file"
    ) as mock_write_to_file, mock.patch(
        "gen3.auth.decode_token"
    ) as mock_decode_token, mock.patch(
        "requests.get"
    ) as mock_request, mock.patch(
        "csv.DictWriter.writerow"
    ) as mock_writerow, mock.patch(
        "csv.DictWriter.writeheader"
    ) as mock_writeheader, mock.patch(
        "builtins.open", mock_open(read_data=read_data)
    ) as mock_file_open:
        mock_access_token.return_value = "new_access_token"
        mock_write_to_file().return_value = True
        mock_decode_token.return_value = decoded_info
        mock_request.return_value = get_mock
        mock_writerow.return_value = None
        mock_writeheader.return_value = None

        input = f"{cwd}/test_data/manifest3.csv"
        output = f"{cwd}/test_data/post_indexing_output.csv"

        auth = gen3.auth.Gen3Auth(refresh_token=test_key)
        records = validate_manifest(input, auth, output)

        mock_file_open.assert_called_with(
            f"{cwd}/test_data/post_indexing_output.csv", mode="w", newline=""
        )
        mock_writeheader.assert_called_once()
        mock_writerow.assert_called_with(
            {
                "acl": "[phs0001]",
                "bucket": "pdcdatastore",
                "protocol": "s3",
                "presigned_url_status": 200,
                "download_status": 200,
                "guid": "255e396f-f1f8-11e9-9a07-0a80fada097c",
            }
        )

        presigned_statuses = []
        download_statuses = []
        for record in records.download_results:
            presigned_statuses.append(record["presigned_url_status"])
            download_statuses.append(record["download_status"])
        assert set(presigned_statuses) == {200} and set(download_statuses) == {200}


def test_validate_manifest_coro_with_401():
    hostname = "test.datacommons.io"
    exp = time.time() + 300
    decoded_info = {"aud": "123", "exp": exp, "iss": f"http://{hostname}"}
    test_key = {
        "api_key": "whatever."  # pragma: allowlist secret
        + base64.urlsafe_b64encode(
            ('{"iss": "http://%s", "exp": %d }' % (hostname, exp)).encode("utf-8")
        ).decode("utf-8")
        + ".whatever"
    }
    mock_response = MagicMock()
    mock_response.status_code = 401  # The status code you want to return
    mock_response.url = "https://exampleurl.com"
    get_mock = MockResponse(401)
    with mock.patch(
        "gen3.auth.get_access_token_with_key"
    ) as mock_access_token, mock.patch(
        "gen3.auth.Gen3Auth._write_to_file"
    ) as mock_write_to_file, mock.patch(
        "gen3.auth.decode_token"
    ) as mock_decode_token, mock.patch(
        "requests.get"
    ) as mock_request, mock.patch(
        "csv.DictWriter.writerow"
    ) as mock_writerow, mock.patch(
        "csv.DictWriter.writeheader"
    ) as mock_writeheader, mock.patch(
        "builtins.open", mock_open(read_data=read_data)
    ) as mock_file_open, mock.patch(
        "gen3.file.Gen3File.get_presigned_url"
    ) as mock_gen3file_get_presigned_url:
        mock_access_token.return_value = "new_access_token"
        mock_write_to_file().return_value = True
        mock_decode_token.return_value = decoded_info
        mock_request.return_value = get_mock
        mock_writerow.return_value = None
        mock_writeheader.return_value = None
        mock_gen3file_get_presigned_url.side_effect = HTTPError(
            "An error occurred", response=mock_response
        )

        input = f"{cwd}/test_data/manifest3.csv"
        output = f"{cwd}/test_data/post_indexing_output.csv"

        auth = gen3.auth.Gen3Auth(refresh_token=test_key)
        records = validate_manifest(input, auth, output)

        mock_file_open.assert_called_with(
            f"{cwd}/test_data/post_indexing_output.csv", mode="w", newline=""
        )
        mock_writeheader.assert_called_once()
        mock_writerow.assert_called_with(
            {
                "acl": "[phs0001]",
                "bucket": "pdcdatastore",
                "protocol": "s3",
                "presigned_url_status": 401,
                "download_status": -1,
                "guid": "255e396f-f1f8-11e9-9a07-0a80fada097c",
            }
        )

        presigned_statuses = []
        download_statuses = []
        for record in records.download_results:
            presigned_statuses.append(record["presigned_url_status"])
            download_statuses.append(record["download_status"])
        assert set(presigned_statuses) == {401} and set(download_statuses) == {-1}


def test_validate_manifest_coro_with_invalid_url():
    hostname = "test.datacommons.io"
    exp = time.time() + 300
    decoded_info = {"aud": "123", "exp": exp, "iss": f"http://{hostname}"}
    test_key = {
        "api_key": "whatever."  # pragma: allowlist secret
        + base64.urlsafe_b64encode(
            ('{"iss": "http://%s", "exp": %d }' % (hostname, exp)).encode("utf-8")
        ).decode("utf-8")
        + ".whatever"
    }
    get_mock = MockResponse(200)
    with mock.patch(
        "gen3.auth.get_access_token_with_key"
    ) as mock_access_token, mock.patch(
        "gen3.auth.Gen3Auth._write_to_file"
    ) as mock_write_to_file, mock.patch(
        "gen3.auth.decode_token"
    ) as mock_decode_token, mock.patch(
        "requests.get"
    ) as mock_request, mock.patch(
        "csv.DictWriter.writerow"
    ) as mock_writerow, mock.patch(
        "csv.DictWriter.writeheader"
    ) as mock_writeheader, mock.patch(
        "builtins.open", mock_open(read_data=read_data2)
    ) as mock_file_open:
        mock_access_token.return_value = "new_access_token"
        mock_write_to_file().return_value = True
        mock_decode_token.return_value = decoded_info
        mock_request.return_value = get_mock
        mock_writerow.return_value = None
        mock_writeheader.return_value = None

        input = f"{cwd}/test_data/manifest3.csv"
        output = f"{cwd}/test_data/post_indexing_output.csv"

        auth = gen3.auth.Gen3Auth(refresh_token=test_key)
        try:
            records = validate_manifest(input, auth, output)
        except ManifestError:
            assert True
