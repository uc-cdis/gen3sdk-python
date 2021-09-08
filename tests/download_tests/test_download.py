import pytest
import json
import base64
import time
import requests
import requests_mock
from dataclasses import asdict
from pathlib import Path

DIR = Path(__file__).resolve().parent

from gen3.tools.download.drs_download import (
    parse_drs_identifier,
    strip_http_url,
    get_drs_object_type,
    extract_filename_from_object_info,
    Manifest,
    DRSObjectType,
    DownloadManager,

    wts_external_oidc,
    resolve_compact_drs_using_dataguids
)


@pytest.fixture
def drs_objects():
    with open(Path(DIR, 'resources/drs_objects.json')) as fin:
        test_data = json.load(fin)
        return test_data['drs_objects']


@pytest.fixture
def drs_resolver_dataguids():
    with open(Path(DIR, 'resources/dataguids_commons1.json')) as fin:
        return json.load(fin)


@pytest.fixture
def wts_oidc():
    with open(Path(DIR, 'resources/wts_oidc.json')) as fin:
        return json.load(fin)


@pytest.mark.parametrize(
    "identifier,expected", [
        ("drs://jcoin.datacommons.io/dg.XXTS/008e6b62-28c0-4659-afec-622340b0ef76",
         ('jcoin.datacommons.io', 'hostname')),
        ("dg.XXTS/008e6b62-28c0-4659-afec-622340b0ef76", ('dg.XXTS', 'compact')),
        ("008e6b62-28c0-4659-afec-622340b0ef76", ('', 'unknown'))
    ]
)
def test_parse_drs_identifier(identifier, expected):
    assert parse_drs_identifier(identifier) == expected


@pytest.mark.parametrize(
    "s,expected", [
        ("https://jcoin.datacommons.io/index/", 'jcoin.datacommons.io'),
        ("http://foo.testgen3.io/index/", 'foo.testgen3.io'),
        ("jcoin.datacommons.io/index/", 'jcoin.datacommons.io'),
        ("jcoin.datacommons.io/index", 'jcoin.datacommons.io'),
    ]
)
def test_strip_http_url(s: str, expected):
    assert strip_http_url(s) == expected


def test_load_manifest():
    object_list = Manifest.load(Path(DIR, 'resources/manifest_test_drs_compact.json'))
    object_list = [asdict(x) for x in object_list]

    with open("expected/manifest_test_drs_compact_object_list.json", "rt") as fin:
        expected = json.load(fin)

    assert expected == object_list


@pytest.mark.parametrize(
    "index,expected", [
        (0, 'TestDataSet1.sav'),
        (1, 'TestDataSet_April2020.sav'),
    ]
)
def test_extract_filename_from_drs_object(drs_objects, index, expected):
    assert extract_filename_from_object_info(drs_objects[index]) == expected


@pytest.mark.parametrize(
    "index,expected", [
        (0, DRSObjectType.object),
    ]
)
def test_get_drs_object_type(drs_objects, index, expected):
    assert get_drs_object_type(drs_objects[index]) == expected


@pytest.mark.parametrize(
    "hostname", [
        ("test.datacommons.io")
    ]
)
def test_get_external_wts_oidc(wts_oidc, hostname):
    with requests_mock.Mocker() as m:
        m.get(f"https://{hostname}/wts/external_oidc/", json=wts_oidc[hostname])
        expected = {
            'test.commons1.io': {
                'base_url': 'https://test.commons1.io',
                'idp': 'test-google',
                'name': 'TEST1 Google Login',
                'refresh_token_expiration': '9 days',
                'urls': [{
                    'name': 'TEST Google Login',
                    'url': 'https://test.datacommons.io/wts/oauth2/authorization_url?idp=test-google'
                }]
            },
            'externaldata.commons2.org': {
                'base_url': 'https://externaldata.commons2.org',
                'idp': 'externaldata-google',
                'name': 'FAIR Repository Google Login',
                'refresh_token_expiration': '7 days',
                'urls': [{
                    'name': 'FAIR Repository Google Login',
                    'url': 'https://test.datacommons.io/wts/oauth2/authorization_url?idp=externaldata-google'
                }]
            }
        }
        results = wts_external_oidc(hostname)
        assert results == expected

        try:
            m.get(f"https://{hostname}/wts/external_oidc/", json={}, status_code=404)
            wts_external_oidc(hostname)
        except Exception as exc:
            assert isinstance(exc, requests.exceptions.HTTPError) == True


def test_resolve_compact_drs_using_dataguids(drs_objects):
    DRS_RESOLVER_RESULTS = {
        "acl": [
            "admin"
        ],
        "authz": [],
        "baseid": "1e6cf3f1-a5af-4543-a1ca-84b41b3221a9",
        "created_date": "2018-06-13T17:16:29.981618",
        "did": "dg.XXTS/00e6cfa9-a183-42f6-bb44-b70347106bbe",
        "file_name": None,
        "form": "object",
        "from_index_service": {
            "host": "https://test.commons1.io/index/",
            "name": "TESTCommons"
        },
        "hashes": {
            "md5": "4fe23cf4c6bdcf930df5765b48d81216"
        },
        "metadata": {},
        "rev": "6046fb9f",
        "size": 9567026,
        "updated_date": "2018-06-13T17:16:29.981629",
        "uploader": None,
        "urls": [
            "gs://test_workflow_testing/topmed_aligner/input_files/176325.0005.recab.cram",
            "s3://test-workflow-testing/topmed-aligner/input-files/176325.0005.recab.cram"
        ],
        "urls_metadata": {
            "gs://test_workflow_testing/topmed_aligner/input_files/176325.0005.recab.cram": {},
            "s3://test-workflow-testing/topmed-aligner/input-files/176325.0005.recab.cram": {}
        },
        "version": None
    }

    object_id = "dg.XXTS/b96018c5-db06-4af8-a195-28e339ba815e"
    identifier = "dg.XXTS"

    DRS_MDS_RESULTS = {
        "host": "https://test.commons1.io/index/",
        "name": "TestCommons",
        "type": "indexd"
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

        m.get(f"https://dataguids.org/mds/metadata/{identifier}", json=DRS_MDS_RESULTS, status_code=404)
        assert resolve_compact_drs_using_dataguids(identifier, object_id) is None

@pytest.mark.parametrize(
    "hostname", [
        ('test.datacommons.io'),
    ]
)
def test_resolve_objects(wts_oidc, hostname):

    # TODO: WIP - need to complete Gen3Auth mocks
    test_key = {
        "api_key": "whatever."
                   + base64.urlsafe_b64encode(
            ('{"iss": "%s", "exp": %d }' % (hostname, time.time() + 300)).encode(
                "utf-8"
            )
        ).decode("utf-8")
                   + ".whatever"
    }

    # with requests_mock.Mocker() as m:
    #     # mock the WTS
    #     m.get(f"https://{hostname}/wts/external_oidc/", json=wts_oidc[hostname])
    #
    #     object_list = Manifest.load(Path(DIR, 'resources/manifest_test_drs_compact.json'))
    #
    #     downloader = DownloadManager(hostname, "")
