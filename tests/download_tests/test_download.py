import pytest
import json
import base64
import time
import requests
import requests_mock
import os
from datetime import datetime
from dataclasses import asdict
from pathlib import Path
from unittest import mock
import gen3

DIR = Path(__file__).resolve().parent

from gen3.tools.download.drs_download import (
    parse_drs_identifier,
    get_drs_object_type,
    get_drs_object_info,
    get_access_methods,
    extract_filename_from_object_info,
    Manifest,
    DRSObjectType,
    Downloadable,
    DownloadManager,
    DownloadStatus,
    wts_external_oidc,
    add_drs_object_info,
    _download,
    _download_obj,
    list_access_in_drs_manifest,
    list_drs_object,
    list_files_in_drs_manifest,
    wts_get_token,
    get_download_url_using_drs,
    download_file_from_url,
)

from gen3.tools.download.drs_resolvers import (
    clean_http_url,
)


@pytest.fixture(scope="session")
def download_dir(tmpdir_factory):
    path = tmpdir_factory.mktemp("drs_download")
    return path


@pytest.fixture
def drs_objects():
    with open(Path(DIR, "resources/drs_objects.json")) as fin:
        test_data = json.load(fin)
        return test_data["drs_objects"]


@pytest.fixture
def drs_object_commons3():
    with open(Path(DIR, "resources/drs_object_commons3.json")) as fin:
        test_data = json.load(fin)
        return test_data


@pytest.fixture
def drs_resolver_dataguids():
    with open(Path(DIR, "resources/dataguids_commons1.json")) as fin:
        return json.load(fin)


@pytest.fixture
def download_test_files():
    data = {}
    with open(Path(DIR, "resources/download_test_data.json")) as fin:
        data = json.load(fin)
        for item in data.values():
            item["content_length"] = str(len(item["content"]))
    return data


@pytest.fixture
def wts_oidc():
    with open(Path(DIR, "resources/wts_oidc.json")) as fin:
        return json.load(fin)


@pytest.fixture
def drs_object_info():
    with open(Path(DIR, "resources/drs_objects.json")) as fin:
        data = json.load(fin)
        return {x["id"]: x for x in data["drs_objects"]}


@pytest.mark.parametrize(
    "identifier,expected",
    [
        (
            "drs://drs.testcommons.me/dg.XXTS/008e6b62-28c0-4659-afec-622340b0ef76",
            (
                "drs.testcommons.me",
                "dg.XXTS/008e6b62-28c0-4659-afec-622340b0ef76",
                "hostname",
            ),
        ),
        (
            "dg.XXTS/008e6b62-28c0-4659-afec-622340b0ef76",
            ("dg.XXTS", "008e6b62-28c0-4659-afec-622340b0ef76", "compact"),
        ),
        ("008e6b62-28c0-4659-afec-622340b0ef76", ("", "", "unknown")),
    ],
)
def test_parse_drs_identifier(identifier, expected):
    assert parse_drs_identifier(identifier) == expected


@pytest.mark.parametrize(
    "s,expected",
    [
        ("https://drs.testcommons.me/index/", "drs.testcommons.me"),
        ("http://foo.testgen3.io/index/", "foo.testgen3.io"),
        ("drs.testcommons.me/index/", "drs.testcommons.me"),
        ("drs.testcommons.me/index", "drs.testcommons.me"),
    ],
)
def test_strip_http_url(s: str, expected):
    assert clean_http_url(s) == expected


def test_add_object_info(drs_object_info):
    with requests_mock.Mocker() as m:
        object_id, info = list(drs_object_info.items())[0]
        m.get(f"https://test.commons1.io/ga4gh/drs/v1/objects/{object_id}", json=info)

        object = Downloadable(object_id=object_id, hostname="test.commons1.io")

        assert add_drs_object_info(object)
        assert object.file_name == "TestDataSet1.sav"
        assert object.file_size == 1566369
        assert object.access_methods == info["access_methods"]

        # test repr

        assert (
            object.__repr__()
            == "(Downloadable: TestDataSet1.sav; 1.57 MB; test.commons1.io; 04/06/2021, 11:22:19)"
        )

        m.get(
            f"https://test.commons1.io/ga4gh/drs/v1/objects/{object_id}",
            json=info,
            status_code=404,
        )
        assert add_drs_object_info(object) is False

        object = Downloadable(object_id=object_id, hostname=None)
        assert add_drs_object_info(object) is False

        # test bundle
        # reset
        m.get(f"https://test.commons1.io/ga4gh/drs/v1/objects/{object_id}", json=info)
        bundle_object_id, bundle_info = list(drs_object_info.items())[9]
        m.get(
            f"https://test.commons1.io/ga4gh/drs/v1/objects/{bundle_object_id}",
            json=bundle_info,
        )

        object = Downloadable(object_id=bundle_object_id, hostname="test.commons1.io")
        assert add_drs_object_info(object)
        assert object.object_type == DRSObjectType.bundle


def test_load_manifest():
    object_list = Manifest.load(Path(DIR, "resources/manifest_test_drs_compact.json"))
    object_list = [asdict(x) for x in object_list]

    with open(
        Path(DIR, "expected/manifest_test_drs_compact_object_list.json"), "rt"
    ) as fin:
        expected = json.load(fin)

    assert expected == object_list
    assert Manifest.load(Path(DIR, "resources/non_existing_file.json")) is None
    assert Manifest.load(Path(DIR, "resources/bad_format.json")) is None


def test_empty_object_list_for_get_access_methods():
    assert get_access_methods(None) == []


def test_failed_wts_get_token():
    with requests_mock.Mocker() as m:
        m.get(f"https://not_available/wts/token/?idp=test", status_code=404)
        assert wts_get_token("not_available", "test", "not really a token") is None

        m.get(
            f"https://not_available/wts/token/?idp=test",
            json={"bad_token": "not a token"},
        )
        assert wts_get_token("not_available", "test", "not really a token") is None


def test_drs_object_info(drs_object_info):
    with requests_mock.Mocker() as m:
        object_id, info = list(drs_object_info.items())[0]
        m.get(f"https://test.commons1.io/ga4gh/drs/v1/objects/{object_id}", json=info)
        assert get_drs_object_info("test.commons1.io", object_id) == info

        m.get(
            f"https://test.commons1.io/ga4gh/drs/v1/objects/{object_id}",
            json=info,
            status_code=404,
        )
        assert get_drs_object_info("test.commons1.io", object_id) is None

        m.get(
            f"https://test.commons1.io/ga4gh/drs/v1/objects/{object_id}",
            json=info,
            status_code=500,
        )
        assert get_drs_object_info("test.commons1.io", object_id) is None


@pytest.mark.parametrize(
    "index,expected",
    [
        (0, "TestDataSet1.sav"),
        (1, "TestDataSet_April2020.sav"),
        (7, "TestDataSet_November2020.csv"),
        (8, None),
    ],
)
def test_extract_filename_from_drs_object(drs_objects, index, expected):
    assert extract_filename_from_object_info(drs_objects[index]) == expected


@pytest.mark.parametrize(
    "index,expected",
    [
        (0, DRSObjectType.object),
        (10, DRSObjectType.bundle),
        (11, DRSObjectType.object),
    ],
)
def test_get_drs_object_type(drs_objects, index, expected):
    assert get_drs_object_type(drs_objects[index]) == expected


@pytest.mark.parametrize("hostname", [("test.datacommons.io")])
def test_get_external_wts_oidc(wts_oidc, hostname):
    with requests_mock.Mocker() as m:
        m.get(f"https://{hostname}/wts/external_oidc/", json=wts_oidc[hostname])
        expected = {
            "test.commons1.io": {
                "base_url": "https://test.commons1.io",
                "idp": "test-google",
                "name": "TEST1 Google Login",
                "refresh_token_expiration": "9 days",
                "urls": [
                    {
                        "name": "TEST Google Login",
                        "url": "https://test.datacommons.io/wts/oauth2/authorization_url?idp=test-google",
                    }
                ],
            },
            "externaldata.commons2.org": {
                "base_url": "https://externaldata.commons2.org",
                "idp": "externaldata-google",
                "name": "FAIR Repository Google Login",
                "refresh_token_expiration": "7 days",
                "urls": [
                    {
                        "name": "FAIR Repository Google Login",
                        "url": "https://test.datacommons.io/wts/oauth2/authorization_url?idp=externaldata-google",
                    }
                ],
            },
        }
        results = wts_external_oidc(hostname)
        assert results == expected

        try:
            m.get(f"https://{hostname}/wts/external_oidc/", json={}, status_code=404)
            wts_external_oidc(hostname)
        except Exception as exc:
            assert isinstance(exc, requests.exceptions.HTTPError) is True


def test_download_file_from_url_failures(download_dir):
    with requests_mock.Mocker() as m:

        m.get(
            url=f"https://test.commons1.io/ga4gh/drs/v1/objects/blah/access/s3",
            headers={"content-length": "10"},
            text="1234560",
        )

        assert (
            download_file_from_url(
                "https://test.commons1.io/ga4gh/drs/v1/objects/blah/access/s3",
                Path("/bad_dir"),
            )
            is False
        )

        assert (
            download_file_from_url(
                "https://test.commons1.io/ga4gh/drs/v1/objects/blah/access/s3",
                download_dir.join("bad"),
            )
            is False
        )

        m.get(
            url=f"https://test.commons1.io/ga4gh/drs/v1/objects/blah/access/s3",
            headers={"content-length": "0"},
            text="1234560",
        )

        assert (
            download_file_from_url(
                "https://test.commons1.io/ga4gh/drs/v1/objects/blah/access/s3",
                download_dir.join("bad"),
            )
            is False
        )

        # HTTP Error
        m.get(
            url=f"https://test.commons1.io/ga4gh/drs/v1/objects/blah/access/s3",
            headers={"content-length": "10"},
            text="1234567890",
            status_code=500,
        )
        assert (
            get_download_url_using_drs("test.commons1.io", "blah", "s3", "bad token")
            is None
        )

        assert (
            download_file_from_url(
                "https://test.commons1.io/ga4gh/drs/v1/objects/blah/access/s3",
                Path("."),
            )
            is False
        )

        # Timeout
        m.get(
            url=f"https://test.commons1.io/ga4gh/drs/v1/objects/blah/access/s3",
            exc=requests.exceptions.Timeout,
        )
        assert (
            get_download_url_using_drs("test.commons1.io", "blah", "s3", "bad token")
            is None
        )

        try:
            download_file_from_url(
                "https://test.commons1.io/ga4gh/drs/v1/objects/blah/access/s3",
                Path("."),
            )
        except Exception as exc:
            assert isinstance(exc, requests.exceptions.Timeout) is True


@pytest.mark.parametrize(
    "hostname",
    [
        ("test.datacommons.io"),
    ],
)
def test_download_objects(
    capsys,
    wts_oidc,
    drs_object_info,
    drs_object_commons3,
    drs_resolver_dataguids,
    download_dir,
    download_test_files,
    hostname,
):
    exp = time.time() + 300
    test_key = {
        "api_key": "whatever."  # pragma: allowlist secret
        + base64.urlsafe_b64encode(
            ('{"iss": "http://%s", "exp": %d }' % (hostname, exp)).encode("utf-8")
        ).decode("utf-8")
        + ".whatever"
    }

    decoded_info = {"aud": "123", "exp": exp, "iss": f"http://{hostname}"}

    commons_url = "test.commons1.io"
    with mock.patch(
        "gen3.tools.download.drs_resolvers.DRS_CACHE",
        str(Path(download_dir, ".drs_cache", "resolved_drs_hosts.json")),
    ), requests_mock.Mocker() as m:
        with mock.patch(
            "gen3.auth.get_access_token_with_key"
        ) as mock_access_token, mock.patch(
            "gen3.auth.Gen3Auth._write_to_file"
        ) as mock_write_to_file, mock.patch(
            "gen3.auth.decode_token"
        ) as mock_decode_token, mock.patch(
            "gen3.tools.download.drs_download.decode_token"
        ) as mock_decode_token_drs:
            mock_access_token.return_value = "new_access_token"
            mock_write_to_file().return_value = True
            mock_decode_token.return_value = decoded_info
            mock_decode_token_drs.return_value = decoded_info
            auth = gen3.auth.Gen3Auth(refresh_token=test_key)
            with capsys.disabled():
                auth.get_access_token()

                # mock the WTS and other responses
                m.get(f"https://{hostname}/wts/external_oidc/", json=wts_oidc[hostname])
                object_list = Manifest.load(
                    Path(DIR, "resources/manifest_test_drs_compact.json")
                )
                m.get(
                    f"https://dataguids.org/index/{drs_resolver_dataguids['did']}",
                    json=drs_resolver_dataguids,
                )
                m.get(
                    f"https://{hostname}/wts/token/?idp=test-google",
                    json={"token": "whatever1"},
                )

                m.get(
                    "http://test.datacommons.io/mds/aggregate/info/dg.XXTS",
                    json={},
                    status_code=404,
                )

                m.get(
                    "https://dataguids.org/index/_dist",
                    json={},
                    status_code=404,
                )
                for object_id, info in drs_object_info.items():
                    m.get(
                        f"https://{commons_url}/ga4gh/drs/v1/objects/{object_id}",
                        json=info,
                    )
                    m.get(
                        f"https://{commons_url}/ga4gh/drs/v1/objects/{object_id}/access/s3",
                        json={
                            "url": f"https://default-download.s3.amazon.com/{object_id}"
                        },
                    )
                for object in object_list:
                    m.get(
                        f"https://default-download.s3.amazon.com/{object.object_id}",
                        headers={
                            "content-length": download_test_files[object.object_id][
                                "content_length"
                            ]
                        },
                        text=download_test_files[object.object_id]["content"],
                    )

                downloader = DownloadManager(hostname, auth, object_list)
                results = downloader.download(
                    object_list=[object_list[0]], save_directory=download_dir
                )

                # test to see if file is downloaded
                for id, item in results.items():
                    assert item.status == "downloaded"
                    with open(download_dir.join(item.filename), "rt") as fin:
                        assert fin.read() == download_test_files[id]["content"]

                # test that when the file name contains '/' ("a/b/<filename>"
                # here), the file is downloaded in subdirectories
                results = downloader.download(
                    object_list=[object_list[4]], save_directory=download_dir
                )
                for id, item in results.items():
                    assert item.status == "downloaded"
                    dir_list = os.listdir(download_dir)
                    assert "a" in dir_list
                    dir_list = os.listdir(download_dir.join("a"))
                    assert "b" in dir_list
                    with open(download_dir.join(item.filename), "rt") as fin:
                        assert fin.read() == download_test_files[id]["content"]

                # test _download manifest
                results = _download(
                    hostname,
                    auth,
                    Path(DIR, "resources/manifest_test_2.json"),
                    download_dir.join("_download"),
                )
                for id, item in results.items():
                    assert item.status == "downloaded"
                    with open(
                        download_dir.join("_download", item.filename), "rt"
                    ) as fin:
                        assert fin.read() == download_test_files[id]["content"]

                # test various other failures

                # Test if manifest has commons not in WTS
                m.get(
                    f"https://test.commons3.io/ga4gh/drs/v1/objects/dg.XX23/b96018c5-db06-4af8-a195-28e339ba815e",
                    json=drs_object_commons3,
                )

                expected = {
                    "dg.XX23/b96018c5-db06-4af8-a195-28e339ba815e": DownloadStatus(
                        filename="TestDataSet1.sav", status="error (no auth)"
                    )
                }

                results = _download(
                    hostname,
                    auth,
                    Path(DIR, "resources/manifest_test_hostname_not_in_wts.json"),
                    download_dir.join("_download"),
                )
                assert expected == results

                # Test if manifest does not exists
                assert (
                    _download(
                        hostname,
                        auth,
                        Path(DIR, "resources/manifest_does_not_exists.json"),
                        download_dir.join("_download"),
                    )
                    is None
                )

                # test download object

                results = _download_obj(
                    hostname,
                    auth,
                    "dg.XXTS/b96018c5-db06-4af8-a195-28e339ba815e",
                    download_dir.join("_download_obj"),
                )
                for id, item in results.items():
                    assert item.status == "downloaded"
                    with open(
                        download_dir.join("_download_obj", item.filename), "rt"
                    ) as fin:
                        assert fin.read() == download_test_files[id]["content"]

                # test timeout
                for object in object_list[:1]:
                    m.get(
                        f"https://default-download.s3.amazon.com/{object.object_id}",
                        exc=requests.exceptions.ConnectTimeout,
                    )

                downloader = DownloadManager(hostname, auth, object_list[:1])
                results = downloader.download(
                    object_list=[object_list[0]], save_directory=download_dir
                )
                assert (
                    results["dg.XXTS/b96018c5-db06-4af8-a195-28e339ba815e"].status
                    == "error"
                )

            # test the Object string representations
            results = object_list[0].__repr__()
            expected = "(Downloadable: TestDataSet1.sav; 1.57 MB; test.commons1.io; 04/06/2021, 11:22:19)"
            assert results == expected

            # test list files
            result = list_files_in_drs_manifest(
                hostname,
                auth,
                Path(DIR, "resources/manifest_test_2.json"),
            )
            captured = capsys.readouterr()
            expected = [
                "TestDataSet1.sav;",
                "1.57",
                "MB;",
                "test.commons1.io;",
                "04/06/2021,",
                "11:22:19",
            ]
            assert expected == captured.out.split()
            assert result is True

            # test non-existent manifest
            res = list_files_in_drs_manifest(hostname, auth, "missing/missing.json")
            assert res is False

            # test non-existent host
            m.get(f"https://nullhost/wts/external_oidc/", json={}, status_code=501)
            m.get(
                "http://nullhost/mds/aggregate/info/dg.XXTST",
                json={},
                status_code=500,
            )
            m.get(
                "https://dataguids.org/index/dg.XXTST/b96018c5-db06-4af8-a195-28e339ba815e",
                json={},
                status_code=500,
            )
            result = list_files_in_drs_manifest(
                "nullhost", auth, Path(DIR, "resources/manifest_test_bad_id.json")
            )
            expected = [
                "not",
                "available;",
                "-1",
                "bytes;",
                "not",
                "resolved;",
                "not",
                "available",
            ]
            assert result is True
            assert expected == capsys.readouterr().out.split()

            # try to download a bad entry
            res = _download(
                "nullhost",
                auth,
                Path(DIR, "resources/manifest_test_bad_id.json"),
                download_dir.join("_download"),
            )
            expected = {
                "dg.XXTST/b96018c5-db06-4af8-a195-28e339ba815e": DownloadStatus(
                    filename=None, status="error (resolving DRS host)"
                )
            }
            assert res == expected

        # test download object with no auth
        m.post(
            f"http://{hostname}/user/credentials/cdis/access_token",
            json={},
            status_code=404,
        )
        results = _download_obj(
            hostname,
            auth,
            "dg.XXTS/b96018c5-db06-4af8-a195-28e339ba815e",
            download_dir.join("_download_obj"),
        )
        assert results is None

        results = _download(
            hostname,
            auth,
            Path(DIR, "resources/manifest_test_2.json"),
            download_dir.join("_download"),
        )
        assert results is None


@pytest.mark.parametrize(
    "hostname,commons_url,manifest_file",
    [("test.datacommons.io", "test.commons1.io", "manifest_test_1.json")],
)
def test_list_auth(
    capsys, wts_oidc, drs_object_info, hostname, commons_url, manifest_file
):
    exp = time.time() + 300
    test_key = {
        "api_key": "whatever."  # pragma: allowlist secret
        + base64.urlsafe_b64encode(
            ('{"iss": "http://%s", "exp": %d }' % (hostname, exp)).encode("utf-8")
        ).decode("utf-8")
        + ".whatever"
    }

    decoded_info = {"aud": "123", "exp": exp, "iss": f"http://{hostname}"}

    with requests_mock.Mocker() as m:
        m.get(f"https://{hostname}/wts/external_oidc/", json=wts_oidc[hostname])
        for object_id, info in drs_object_info.items():
            m.get(f"https://{commons_url}/ga4gh/drs/v1/objects/{object_id}", json=info)

        m.get(f"https://{hostname}/wts/token/?idp=test-google", json={})
        authz = {
            "authz": {
                "/dictionary_page": [
                    {"method": "access", "service": "dictionary_page"}
                ],
                "/programs/open": [
                    {"method": "read", "service": "*"},
                    {"method": "read-storage", "service": "*"},
                ],
                "/programs/open/projects": [
                    {"method": "read", "service": "*"},
                    {"method": "read-storage", "service": "*"},
                ],
                "/programs/open/projects/TEST1": [
                    {"method": "read", "service": "*"},
                    {"method": "read-storage", "service": "*"},
                ],
                "/programs/open/projects/NOBE": [
                    {"method": "read", "service": "*"},
                    {"method": "read-storage", "service": "*"},
                ],
                "/programs/open/projects/Capturing_Errors_in_code": [
                    {"method": "read", "service": "*"},
                    {"method": "read-storage", "service": "*"},
                ],
                "/sower": [{"method": "access", "service": "job"}],
                "/workspace": [{"method": "access", "service": "jupyterhub"}],
            }
        }
        m.get(f"https://{hostname}/user/user", json=authz)

        with mock.patch(
            "gen3.auth.get_access_token_with_key"
        ) as mock_access_token, mock.patch(
            "gen3.auth.Gen3Auth._write_to_file"
        ) as mock_write_to_file, mock.patch(
            "gen3.auth.decode_token"
        ) as mock_decode_token, mock.patch(
            "gen3.tools.download.drs_download.decode_token"
        ) as mock_decode_token_drs:
            mock_access_token.return_value = "new_access_token"
            mock_write_to_file().return_value = True
            mock_decode_token.return_value = decoded_info
            mock_decode_token_drs.return_value = decoded_info
            auth = gen3.auth.Gen3Auth(refresh_token=test_key)

            result = list_access_in_drs_manifest(
                hostname, auth, Path(DIR, f"resources/{manifest_file}")
            )
            captured = capsys.readouterr()
            expected = """───────────────────────────────────────────────────────────────────────────────────────────────────────
Access for test.datacommons.io:
      /dictionary_page                                       :                                   access
      /programs/open                                         :                        read read-storage
      /programs/open/projects                                :                        read read-storage
      /programs/open/projects/TEST1                          :                        read read-storage
      /programs/open/projects/NOBE                           :                        read read-storage
      /programs/open/projects/Capturing_Errors_in_code       :                        read read-storage
      /sower                                                 :                                   access
      /workspace                                             :                                   access
"""
            assert result is True
            assert captured.out == expected

            # Test missing manifest file
            result = list_access_in_drs_manifest(
                hostname, auth, Path(DIR, f"resources/non_existent_file.json")
            )
            assert result is False

            # Test HTTP failure of user auth
            m.get(f"https://{hostname}/user/user", json={}, status_code=404)
            result = list_access_in_drs_manifest(
                hostname, auth, Path(DIR, f"resources/{manifest_file}")
            )
            captured = capsys.readouterr()
            results = captured.out.split()
            expected = [
                "───────────────────────────────────────────────────────────────────────────────────────────────────────",
                "Access",
                "for",
                "test.datacommons.io:",
                "No",
                "access",
            ]
            assert results == expected


def test_download_status_repr_and_str():
    download1 = DownloadStatus(
        filename="test.csv",
        status="downloaded",
        start_time=datetime.fromisoformat("2011-11-04T00:05:23"),
        end_time=datetime.fromisoformat("2011-11-04T00:07:12"),
    )

    expected = "filename: test.csv; status: downloaded; start_time: 11/04/2011, 00:05:23; end_time: 11/04/2011, 00:07:12"

    results = download1.__repr__()
    assert results == expected

    results = download1.__str__()
    assert results == expected


def test_no_gen3_auth():
    hostname = "test.datacommons.io"
    test_key = {
        "api_key": "whatever."  # pragma: allowlist secret
        + base64.urlsafe_b64encode(
            ('{"iss": "http://%s", "exp": %d }' % (hostname, time.time() + 300)).encode(
                "utf-8"
            )
        ).decode("utf-8")
        + ".whatever"
    }

    with requests_mock.Mocker() as m:
        m.post(
            f"http://{hostname}/user/credentials/cdis/access_token",
            json={},
            status_code=501,
        )
        with mock.patch(
            "gen3.auth.Gen3Auth._write_to_file"
        ) as mock_write_to_file:  # do not cache fake token
            mock_write_to_file().return_value = False
            auth = gen3.auth.Gen3Auth(refresh_token=test_key)
            result = list_access_in_drs_manifest(
                hostname, auth, Path(DIR, f"resources/manifest_test_1.json")
            )
            assert result is False
            result = list_files_in_drs_manifest(
                hostname, auth, Path(DIR, f"resources/manifest_test_1.json")
            )
            assert result is False


@pytest.mark.parametrize(
    "hostname,commons_url,manifest_file,expected",
    [
        (
            "test.datacommons.io",
            "test.testcommons1.org",
            "manifest_test_1.json",
            [
                "TestDataSet1.sav",
                "1.57",
                "MB",
                "test.testcommons1.org",
                "04/06/2021,",
                "11:22:19",
            ],
        )
    ],
)
def test_list_no_auth(
    capsys,
    wts_oidc,
    drs_object_info,
    drs_resolver_dataguids,
    hostname,
    commons_url,
    manifest_file,
    expected,
):

    test_key = {
        "api_key": "whatever."  # pragma: allowlist secret
        + base64.urlsafe_b64encode(
            ('{"iss": "http://%s", "exp": %d }' % (hostname, time.time() + 300)).encode(
                "utf-8"
            )
        ).decode("utf-8")
        + ".whatever"
    }

    with requests_mock.Mocker() as m:
        m.get(f"https://{hostname}/wts/external_oidc/", json=wts_oidc[hostname])
        m.post(
            f"http://{hostname}/user/credentials/cdis/access_token",
            json={"access_token": "test_access_token"},
        )
        for object_id, info in drs_object_info.items():
            m.get(f"https://{commons_url}/ga4gh/drs/v1/objects/{object_id}", json=info)

        with mock.patch("gen3.auth.Gen3Auth._write_to_file") as mock_write_to_file:
            mock_write_to_file().return_value = False
            auth = gen3.auth.Gen3Auth(refresh_token=test_key)

            # test getting auth error
            # test no auth
            m.post(
                f"http://{hostname}/user/credentials/cdis/access_token",
                json={"access_token": "test_access_token"},
                status_code=401,
            )

            result = list_drs_object(
                hostname, auth, "dg.XXTS/b96018c5-db06-4af8-a195-28e339ba815e"
            )
            assert result is False


@pytest.mark.parametrize(
    "hostname",
    [
        ("test.datacommons.io"),
    ],
)
def test_unpackage_objects(
    capsys,
    wts_oidc,
    drs_object_info,
    drs_resolver_dataguids,
    download_dir,
    download_test_files,
    hostname,
):

    exp = time.time() + 300
    test_key = {
        "api_key": "whatever."  # pragma: allowlist secret
        + base64.urlsafe_b64encode(
            ('{"iss": "http://%s", "exp": %d }' % (hostname, exp)).encode("utf-8")
        ).decode("utf-8")
        + ".whatever"
    }

    decoded_info = {"aud": "123", "exp": exp, "iss": f"http://{hostname}"}

    commons_url = "test.commons1.io"
    with mock.patch(
        "gen3.tools.download.drs_resolvers.DRS_CACHE",
        str(Path(download_dir, ".drs_cache", "resolved_drs_hosts.json")),
    ), requests_mock.Mocker() as m:
        with mock.patch(
            "gen3.auth.get_access_token_with_key"
        ) as mock_access_token, mock.patch(
            "gen3.auth.Gen3Auth._write_to_file"
        ) as mock_write_to_file, mock.patch(
            "gen3.auth.decode_token"
        ) as mock_decode_token, mock.patch(
            "gen3.tools.download.drs_download.decode_token"
        ) as mock_decode_token_drs:
            mock_access_token.return_value = "new_access_token"
            mock_write_to_file().return_value = True
            mock_decode_token.return_value = decoded_info
            mock_decode_token_drs.return_value = decoded_info
            auth = gen3.auth.Gen3Auth(refresh_token=test_key)
            with capsys.disabled():
                auth.get_access_token()

                # mock the WTS and other responses
                m.get(f"https://{hostname}/wts/external_oidc/", json=wts_oidc[hostname])
                object_list = Manifest.load(
                    Path(DIR, "resources/manifest_package.json")
                )
                mds_response = json.load(open(str(DIR) + "/resources/mds_package.json"))
                m.get(
                    f"https://dataguids.org/index/{drs_resolver_dataguids['did']}",
                    json=drs_resolver_dataguids,
                )
                m.get(
                    f"https://{hostname}/wts/token/?idp=test-google",
                    json={"token": "whatever1"},
                )

                m.get(
                    "http://test.datacommons.io/mds/aggregate/info/dg.XXTS",
                    json={},
                    status_code=404,
                )

                m.get(
                    "https://dataguids.org/index/_dist",
                    json={},
                    status_code=404,
                )
                for object_id, info in drs_object_info.items():
                    m.get(
                        f"https://{commons_url}/ga4gh/drs/v1/objects/{object_id}",
                        json=info,
                    )
                    m.get(
                        f"http://test.datacommons.io/mds/metadata/{object_id}",
                        json=mds_response.get(object_id),
                    )
                    m.get(
                        f"https://{commons_url}/ga4gh/drs/v1/objects/{object_id}/access/s3",
                        json={
                            "url": f"https://default-download.s3.amazon.com/{object_id}"
                        },
                    )
                for object in object_list:
                    # we used base64 to store the ZIP files bytes in json, so
                    # we must decode in the response
                    download_test_files[object.object_id]["content"] = base64.b64decode(
                        download_test_files[object.object_id]["content"]
                    )
                    m.get(
                        f"https://default-download.s3.amazon.com/{object.object_id}",
                        headers={
                            "content-length": str(
                                len(download_test_files[object.object_id]["content"])
                            )
                        },
                        content=download_test_files[object.object_id]["content"],
                    )

                downloader = DownloadManager(hostname, auth, object_list)
                results = downloader.download(
                    object_list=[object_list[0]], save_directory=download_dir
                )

                # test that we downloaded the file and that the zip is unpacked
                for id, item in results.items():
                    assert item.status == "downloaded"
                    dir_list = os.listdir(download_dir)
                    assert "b.txt" in dir_list and "c.txt" in dir_list
                    with open(download_dir.join(item.filename), "rb") as fin:
                        assert fin.read() == download_test_files[id]["content"]

                    # clean up download directory for other tests
                    os.remove(download_dir.join("b.txt"))
                    os.remove(download_dir.join("c.txt"))

                # test that we download the file that is not a package in mds and it's not unpacked
                results = downloader.download(
                    object_list=[object_list[1]], save_directory=download_dir
                )
                for id, item in results.items():
                    assert item.status == "downloaded"
                    dir_list = os.listdir(download_dir)
                    assert "b.txt" not in dir_list and "c.txt" not in dir_list
                    with open(download_dir.join(item.filename), "rb") as fin:
                        assert fin.read() == download_test_files[id]["content"]

                # test that we don't undpack when entry is not in mds
                results = downloader.download(
                    object_list=[object_list[2]], save_directory=download_dir
                )
                for id, item in results.items():
                    assert item.status == "downloaded"
                    dir_list = os.listdir(download_dir)
                    assert "b.txt" not in dir_list and "c.txt" not in dir_list
                    with open(download_dir.join(item.filename), "rb") as fin:
                        assert fin.read() == download_test_files[id]["content"]

                # test file that is in the mds but is not the correct extension
                results = downloader.download(
                    object_list=[object_list[3]], save_directory=download_dir
                )
                for id, item in results.items():
                    assert item.status == "downloaded"
                    dir_list = os.listdir(download_dir)
                    assert "b.txt" not in dir_list and "c.txt" not in dir_list
                    with open(download_dir.join(item.filename), "rb") as fin:
                        assert fin.read() == download_test_files[id]["content"]

                # test that file is correct extension and is package in mds
                # but extraction doesn't work because the file is corrupted
                results = downloader.download(
                    object_list=[object_list[4]], save_directory=download_dir
                )
                for id, item in results.items():
                    assert item.status == "error"
                    dir_list = os.listdir(download_dir)
                    assert "b.txt" not in dir_list and "c.txt" not in dir_list
                    with open(download_dir.join(item.filename), "rb") as fin:
                        assert fin.read() == download_test_files[id]["content"]

                # test that when the file name contains '/' ("a/b/<filename>"
                # here), the file is downloaded and extracted in subdirectories
                results = downloader.download(
                    object_list=[object_list[5]], save_directory=download_dir
                )
                for id, item in results.items():
                    assert item.status == "downloaded"
                    dir_list = os.listdir(download_dir)
                    assert "a" in dir_list
                    dir_list = os.listdir(download_dir.join("a"))
                    assert "b" in dir_list
                    dir_list = os.listdir(download_dir.join("a", "b"))
                    assert "b.txt" in dir_list and "c.txt" in dir_list
                    with open(download_dir.join(item.filename), "rb") as fin:
                        assert fin.read() == download_test_files[id]["content"]
