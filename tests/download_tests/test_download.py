import pytest
import json
import base64
import time
import requests
import requests_mock
from unittest.mock import patch
from dataclasses import asdict
from pathlib import Path
import gen3

DIR = Path(__file__).resolve().parent

from gen3.tools.download.drs_download import (
    parse_drs_identifier,
    strip_http_url,
    get_drs_object_type,
    get_drs_object_info,
    get_access_methods,
    extract_filename_from_object_info,
    Manifest,
    DRSObjectType,
    Downloadable,
    DownloadManager,
    wts_external_oidc,
    resolve_compact_drs_using_dataguids,
    add_drs_object_info,
    _download,
    _download_obj,
    _listfiles,
    list_files_in_workspace_manifest,
    wts_get_token,
    get_download_url_using_drs,
    get_download_url,
    download_file_from_url,
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
    assert strip_http_url(s) == expected


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


def test_resolve_compact_drs_using_dataguids(drs_objects):
    DRS_RESOLVER_RESULTS = {
        "acl": ["admin"],
        "authz": [],
        "baseid": "1e6cf3f1-a5af-4543-a1ca-84b41b3221a9",
        "created_date": "2018-06-13T17:16:29.981618",
        "did": "dg.XXTS/00e6cfa9-a183-42f6-bb44-b70347106bbe",
        "file_name": None,
        "form": "object",
        "from_index_service": {
            "host": "https://test.commons1.io/index/",
            "name": "TESTCommons",
        },
        "hashes": {"md5": "4fe23cf4c6bdcf930df5765b48d81216"},
        "metadata": {},
        "rev": "6046fb9f",
        "size": 9567026,
        "updated_date": "2018-06-13T17:16:29.981629",
        "uploader": None,
        "urls": [
            "gs://test_workflow_testing/topmed_aligner/input_files/176325.0005.recab.cram",
            "s3://test-workflow-testing/topmed-aligner/input-files/176325.0005.recab.cram",
        ],
        "urls_metadata": {
            "gs://test_workflow_testing/topmed_aligner/input_files/176325.0005.recab.cram": {},
            "s3://test-workflow-testing/topmed-aligner/input-files/176325.0005.recab.cram": {},
        },
        "version": None,
    }

    object_id = "dg.XXTS/b96018c5-db06-4af8-a195-28e339ba815e"
    identifier = "dg.XXTS"

    DRS_MDS_RESULTS = {
        "host": "https://test.commons1.io/index/",
        "name": "TestCommons",
        "type": "indexd",
    }

    with requests_mock.Mocker() as m:
        m.get(f"https://dataguids.org/{object_id}", json=DRS_RESOLVER_RESULTS)
        results = resolve_compact_drs_using_dataguids(identifier, object_id)
        expected = "test.commons1.io"
        assert results == expected

        object_id = "dg.TEST/00e6cfa9-a183-42f6-bb44-b70347106bbe"
        identifier = "dg.TEST"
        m.get(f"https://dataguids.org/{object_id}", json={}, status_code=404)
        m.get(f"https://dataguids.org/mds/metadata/{identifier}", json=DRS_MDS_RESULTS)
        results = resolve_compact_drs_using_dataguids(identifier, object_id)
        expected = "test.commons1.io"
        assert results == expected

        m.get(f"https://dataguids.org/{object_id}", json={}, status_code=500)
        m.get(f"https://dataguids.org/mds/metadata/{identifier}", json=DRS_MDS_RESULTS)
        assert resolve_compact_drs_using_dataguids(identifier, object_id) == None

        m.get(
            f"https://dataguids.org/mds/metadata/{identifier}",
            json=DRS_MDS_RESULTS,
            status_code=404,
        )
        assert resolve_compact_drs_using_dataguids(identifier, object_id) is None

        object_id = "dg.TEST/00e6cfa9-a183-42f6-bb44-b70347106bbe"
        identifier = "dg.TEST"
        m.get(f"https://dataguids.org/{object_id}", json={}, status_code=404)
        m.get(
            f"https://dataguids.org/mds/metadata/{identifier}",
            json=DRS_MDS_RESULTS,
            status_code=500,
        )
        assert resolve_compact_drs_using_dataguids(identifier, object_id) is None


@pytest.mark.parametrize(
    "hostname",
    [
        ("test.datacommons.io"),
    ],
)
def test_download_objects(
    gen3_auth,
    wts_oidc,
    drs_object_info,
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
    remote_host = ""

    with requests_mock.Mocker() as m:
        # mock the initial credentials request
        m.post(
            f"http://{hostname}/user/credentials/cdis/access_token",
            json={
                "access_token": "eyJhbGciOiJIUzI1NiJ9.eyJSb2xlIjoiQWRtaW4iLCJJc3N1ZXIiOiJJc3N1ZXIiLCJVc2VybmFtZSI6IkphdmFJblVzZSIsImV4cCI6MTYzMTI5MTI0NywiaWF0IjoxNjMxMjkxMjQ3fQ.DRcKE6Zr5aDzrh2Hz-0Zo8N0kGNQfgWIwmt6W_MTkls"
            },
        )
        with patch(
            "gen3.auth.Gen3Auth._write_to_file"
        ) as mock_write_to_file:  # do not cache fake token
            mock_write_to_file().return_value = True
            auth = gen3.auth.Gen3Auth(refresh_token=test_key)
            token = auth.get_access_token()

        # mock the WTS and other responses
        m.get(f"https://{hostname}/wts/external_oidc/", json=wts_oidc[hostname])
        object_list = Manifest.load(
            Path(DIR, "resources/manifest_test_drs_compact.json")
        )
        m.get(
            f"https://dataguids.org/{drs_resolver_dataguids['did']}",
            json=drs_resolver_dataguids,
        )
        m.get(
            f"https://{hostname}/wts/token/?idp=test-google",
            json={"token": "whatever1"},
        )

        for object_id, info in drs_object_info.items():
            m.get(f"https://{commons_url}/ga4gh/drs/v1/objects/{object_id}", json=info)
            m.get(
                f"https://{commons_url}/ga4gh/drs/v1/objects/{object_id}/access/s3",
                json={"url": f"https://default-download.s3.amazon.com/{object_id}"},
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
            assert item["status"] == "downloaded"
            with open(download_dir.join(item["file_name"]), "rt") as fin:
                assert fin.read() == download_test_files[id]["content"]

        # test _download manifest

        results = _download(
            hostname,
            auth,
            Path(DIR, "resources/manifest_test_2.json"),
            download_dir.join("_download"),
        )
        for id, item in results.items():
            assert item["status"] == "downloaded"
            with open(download_dir.join("_download", item["file_name"]), "rt") as fin:
                assert fin.read() == download_test_files[id]["content"]
        # test various other failures

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
            assert item["status"] == "downloaded"
            with open(
                download_dir.join("_download_obj", item["file_name"]), "rt"
            ) as fin:
                assert fin.read() == download_test_files[id]["content"]

        # test listfiles

        _listfiles(hostname, auth, "resources/manifest_test_2.json")

        list_files_in_workspace_manifest(
            hostname, auth, "resources/manifest_test_2.json"
        )
