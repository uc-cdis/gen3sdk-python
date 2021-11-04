import pytest
import json
import base64
import time
import requests
import requests_mock
from datetime import datetime, timezone, timedelta
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
    _list_access,
    list_files_in_workspace_manifest,
    wts_get_token,
    get_download_url_using_drs,
    get_download_url,
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
            "drs://jcoin.datacommons.io/dg.XXTS/008e6b62-28c0-4659-afec-622340b0ef76",
            (
                "jcoin.datacommons.io",
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
        ("https://jcoin.datacommons.io/index/", "jcoin.datacommons.io"),
        ("http://foo.testgen3.io/index/", "foo.testgen3.io"),
        ("jcoin.datacommons.io/index/", "jcoin.datacommons.io"),
        ("jcoin.datacommons.io/index", "jcoin.datacommons.io"),
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
            == "(Downloadable: TestDataSet1.sav 1.57 MB test.commons1.io 04/06/2021, 11:22:19)"
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
        (6, "TestDataSet_November2020.csv"),
        (7, None),
    ],
)
def test_extract_filename_from_drs_object(drs_objects, index, expected):
    assert extract_filename_from_object_info(drs_objects[index]) == expected


@pytest.mark.parametrize(
    "index,expected",
    [
        (0, DRSObjectType.object),
        (9, DRSObjectType.bundle),
        (10, DRSObjectType.object),
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

        assert get_download_url("test.commons1.io", "blah", "s3", "bad token") is None

        # Timeout
        m.get(
            url=f"https://test.commons1.io/ga4gh/drs/v1/objects/blah/access/s3",
            exc=requests.exceptions.Timeout,
        )
        assert get_download_url("test.commons1.io", "blah", "s3", "bad token") is None

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
    gen3_auth,
    wts_oidc,
    drs_object_info,
    drs_object_commons3,
    drs_resolver_dataguids,
    download_dir,
    download_test_files,
    hostname,
):
    test_key = {
        "api_key": "whatever."
        + base64.urlsafe_b64encode(
            ('{"iss": "http://%s", "exp": %d }' % (hostname, time.time() + 300)).encode(
                "utf-8"
            )
        ).decode("utf-8")
        + ".whatever"
    }

    commons_url = "test.commons1.io"
    with mock.patch(
        "gen3.tools.download.drs_resolvers.DRS_CACHE",
        str(Path(download_dir, ".drs_cache", "resolved_drs_hosts.json")),
    ):
        with requests_mock.Mocker() as m:
            # mock the initial credentials request
            with capsys.disabled():
                m.post(
                    f"http://{hostname}/user/credentials/cdis/access_token",
                    json={
                        "access_token": "eyJhbGciOiJIUzI1NiJ9.eyJSb2xlIjoiQWRtaW4iLCJJc3N1ZXIiOiJJc3N1ZXIiLCJVc2VybmFtZSI6IkphdmFJblVzZSIsImV4cCI6MTYzMTI5MTI0NywiaWF0IjoxNjMxMjkxMjQ3fQ.DRcKE6Zr5aDzrh2Hz-0Zo8N0kGNQfgWIwmt6W_MTkls"
                    },
                )
                with mock.patch(
                    "gen3.auth.Gen3Auth._write_to_file"
                ) as mock_write_to_file:  # do not cache fake token
                    mock_write_to_file().return_value = True
                    auth = gen3.auth.Gen3Auth(refresh_token=test_key)
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

                # test listfiles

            # test the Object string representations
            results = object_list[0].__repr__()
            expected = "(Downloadable: TestDataSet1.sav 1.57 MB test.commons1.io 04/06/2021, 11:22:19)"
            assert results == expected

            # test list files
            result = list_files_in_workspace_manifest(
                hostname,
                auth,
                Path(DIR, "resources/manifest_test_2.json"),
            )
            captured = capsys.readouterr()
            expected = [
                "TestDataSet1.sav",
                "1.57",
                "MB",
                "test.commons1.io",
                "04/06/2021,",
                "11:22:19",
            ]
            assert expected == captured.out.split()
            assert result is True

            # test non-existent manifest
            res = list_files_in_workspace_manifest(
                hostname, auth, "missing/missing.json"
            )
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
            result = list_files_in_workspace_manifest(
                "nullhost", auth, Path(DIR, "resources/manifest_test_bad_id.json")
            )
            expected = [
                "not",
                "available",
                "-1",
                "bytes",
                "not",
                "resolved",
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


@pytest.mark.parametrize(
    "hostname,commons_url,manifest_file",
    [("test.datacommons.io", "test.commons1.io", "manifest_test_1.json")],
)
def test_list_auth(
    capsys, wts_oidc, drs_object_info, hostname, commons_url, manifest_file
):
    test_key = {
        "api_key": "whatever."
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
            json={
                "access_token": "eyJhbGciOiJIUzI1NiJ9.eyJSb2xlIjoiQWRtaW4iLCJJc3N1ZXIiOiJJc3N1ZXIiLCJVc2VybmFtZSI6IkphdmFJblVzZSIsImV4cCI6MTYzMTI5MTI0NywiaWF0IjoxNjMxMjkxMjQ3fQ.DRcKE6Zr5aDzrh2Hz-0Zo8N0kGNQfgWIwmt6W_MTkls"
            },
        )

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
                "/programs/open/projects/BACPAC": [
                    {"method": "read", "service": "*"},
                    {"method": "read-storage", "service": "*"},
                ],
                "/programs/open/projects/HOPE": [
                    {"method": "read", "service": "*"},
                    {"method": "read-storage", "service": "*"},
                ],
                "/programs/open/projects/Preventing_Opioid_Use_Disorder": [
                    {"method": "read", "service": "*"},
                    {"method": "read-storage", "service": "*"},
                ],
                "/sower": [{"method": "access", "service": "job"}],
                "/workspace": [{"method": "access", "service": "jupyterhub"}],
            }
        }
        m.get(f"https://{hostname}/user/user", json=authz)
        with mock.patch(
            "gen3.auth.Gen3Auth._write_to_file"
        ) as mock_write_to_file:  # do not cache fake token
            mock_write_to_file().return_value = True
            auth = gen3.auth.Gen3Auth(refresh_token=test_key)

            result = _list_access(
                hostname, auth, Path(DIR, f"resources/{manifest_file}")
            )
            captured = capsys.readouterr()
            expected = """Access for test.datacommons.io
      /dictionary_page: access
      /programs/open: read read-storage
      /programs/open/projects: read read-storage
      /programs/open/projects/BACPAC: read read-storage
      /programs/open/projects/HOPE: read read-storage
      /programs/open/projects/Preventing_Opioid_Use_Disorder: read read-storage
      /sower: access
      /workspace: access
"""
            assert result is True
            assert captured.out == expected

            # Test missing manifest file
            result = _list_access(
                hostname, auth, Path(DIR, f"resources/non_existent_file.json")
            )
            assert result is False


def test_download_status_repr_and_str():
    download1 = DownloadStatus(
        filename="test.csv",
        status="downloaded",
        startTime=datetime.fromisoformat("2011-11-04T00:05:23"),
        endTime=datetime.fromisoformat("2011-11-04T00:07:12"),
    )

    results = download1.__repr__()
    expected = "filename: test.csv status: downloaded startTime: 11/04/2011, 00:05:23 endTime: 11/04/2011, 00:07:12"
    assert results == expected

    results = download1.__str__()
    expected = "filename: test.csv status: downloaded startTime: 11/04/2011, 00:05:23 endTime: 11/04/2011, 00:07:12"
    assert results == expected


def test_no_gen3_auth():
    hostname = "test.datacommons.io"
    test_key = {
        "api_key": "whatever."
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
            print("done")
            result = _list_access(
                hostname, auth, Path(DIR, f"resources/manifest_test_1.json")
            )
            assert result is False
            result = list_files_in_workspace_manifest(
                hostname, auth, Path(DIR, f"resources/manifest_test_1.json")
            )
            assert result is False
