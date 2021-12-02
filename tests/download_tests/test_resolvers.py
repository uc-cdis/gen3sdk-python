import pytest
import json
import requests_mock
from unittest import mock
from pathlib import Path
import shutil

DIR = Path(__file__).resolve().parent

from gen3.tools.download.drs_resolvers import (
    resolve_drs,
    create_local_drs_cache,
    append_to_local_drs_cache,
    resolve_drs_from_local_cache,
    resolve_compact_drs_using_indexd_dist,
    resolve_compact_drs_using_dataguids,
    resolve_drs_using_commons_mds,
    resolve_drs_via_list,
)


@pytest.fixture(scope="session")
def download_dir(tmpdir_factory):
    return tmpdir_factory.mktemp("drs_resolvers")


@pytest.fixture(scope="session")
def index_dist():
    with open(Path(DIR, "resources/index_dist.json")) as fin:
        return json.load(fin)


@pytest.fixture(scope="session")
def dataguids_object_id_request():
    with open(Path(DIR, "resources/dataguids_commons1.json")) as fin:
        return json.load(fin)


@pytest.mark.parametrize(
    "identifier,expected, write_cache",
    [
        ("dg.F82A1A", "data.kidsfirstdrc.org", False),
        ("dg.ANV0", "gen3.theanvil.io", True),
        ("dg.5656", None, True),
    ],
)
def test_resolve_drs_with_indexd_dist(
    download_dir, index_dist, identifier, expected, write_cache
):
    with requests_mock.Mocker() as m:
        m.get(f"https://dataguids_mock.org/index/_dist", json=index_dist)

        with mock.patch(
            "gen3.tools.download.drs_resolvers.DRS_CACHE",
            str(Path(download_dir, ".drs_cache", "resolved_drs_hosts.json")),
        ):
            results = resolve_compact_drs_using_indexd_dist(
                identifier,
                cache_results=write_cache,
                resolver_hostname="https://dataguids_mock.org",
            )
            assert results == expected


@pytest.mark.parametrize(
    "identifier,expected",
    [
        ("dg.ANV0", None),
    ],
)
def test_resolve_drs_with_indexd_dist_http_error(index_dist, identifier, expected):
    with requests_mock.Mocker() as m:
        m.get(
            f"https://dataguids_mock.org/index/_dist", json=index_dist, status_code=500
        )
        results = resolve_compact_drs_using_indexd_dist(
            identifier,
            cache_results=False,
            resolver_hostname="https://dataguids_mock.org",
        )
        assert results == expected


@pytest.mark.parametrize(
    "identifier,expected",
    [
        ("dg.F82A1A", "data.kidsfirstdrc.org"),
        ("dg.H35L", "externaldata.healdata.org"),
        ("dg.5656", None),
    ],
)
def test_resolve_drs_from_local_cache(download_dir, identifier, expected):
    with mock.patch(
        "gen3.tools.download.drs_resolvers.DRS_CACHE",
        str(Path(download_dir, ".drs_cache", "resolved_drs_hosts.json")),
    ):
        results = resolve_drs_from_local_cache(identifier, None)
        assert results == expected


def test_resolve_drs_from_local_cache_exceptions(download_dir):
    results = resolve_drs_from_local_cache("dg.XXTS", None, cache_dir=None)
    assert results is None

    with mock.patch(
        "gen3.tools.download.drs_resolvers.DRS_CACHE",
        str(Path(download_dir, ".drs_cache", "resolved_drs_hosts.json")),
    ):
        results = resolve_drs_from_local_cache("dg.NOOP", None)
        assert results is None

    # test with a cache file that does not exists
    with mock.patch(
        "gen3.tools.download.drs_resolvers.DRS_CACHE",
        str(Path(download_dir, ".drs_cache", "resolved_drs_hosts_not_found.json")),
    ):
        results = resolve_drs_from_local_cache("dg.H35L", None)
        assert results is None

    # test with cache in un-parsable format
    with mock.patch(
        "gen3.tools.download.drs_resolvers.DRS_CACHE",
        str(Path(DIR, "resources", "bad_format.json")),
    ):
        results = resolve_drs_from_local_cache("dg.H35L", None)
        assert results is None


def test_write_cache_to_unwritable_dir():
    assert create_local_drs_cache({}, Path("/.drs_cache")) is False


def test_cache_expired(download_dir):

    src = Path(DIR, "resources", "expired_drs_host_cache.json")
    dst = Path(download_dir, ".drs_cache", "expired_drs_host_cache.json")
    shutil.copy(src, dst)
    with mock.patch(
        "gen3.tools.download.drs_resolvers.DRS_CACHE",
        str(dst),
    ):
        assert resolve_drs_from_local_cache("dg.XXTS") is None


def test_append_cache(download_dir):
    cache_file = Path(download_dir, ".drs_cache", "resolved_drs_hosts_append.json")
    data_first = {
        "dg.XXTS": {
            "host": "https://test.commons1.io/index/",
            "name": "TestCommons",
            "type": "indexd",
        }
    }
    data_second = {
        "dg.6VTS": {
            "host": "https://jcoin.datacommons.io/ga4gh/drs/v1/objects/",
            "name": "JCOIN",
            "type": "DRS",
        }
    }
    with mock.patch("gen3.tools.download.drs_resolvers.DRS_CACHE", str(cache_file)):
        assert append_to_local_drs_cache(data_first)
        with open(cache_file, "rt") as fin:
            results = json.load(fin)
            assert results["cache"] == data_first
        assert append_to_local_drs_cache(data_second)
        with open(cache_file, "rt") as fin:
            results = json.load(fin)
            assert results["cache"] == {**data_first, **data_second}

    with mock.patch(
        "gen3.tools.download.drs_resolvers.DRS_CACHE",
        str(Path(DIR, "resources", "bad_format.json")),
    ):
        assert append_to_local_drs_cache(data_second) is False

    assert append_to_local_drs_cache(data_second, Path("/.drs_cache")) is False

    cache_file.chmod(0o000)
    assert append_to_local_drs_cache(data_second, cache_file) is False
    cache_file.chmod(0o666)


def test_resolve_compact_drs_using_dataguids(download_dir):
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
    with mock.patch(
        "gen3.tools.download.drs_resolvers.DRS_CACHE",
        str(Path(download_dir, ".drs_cache", "resolved_drs_hosts.json")),
    ):
        with requests_mock.Mocker() as m:
            m.get(
                f"https://dataguids_mock.org/index/{object_id}",
                json=DRS_RESOLVER_RESULTS,
            )
            results = resolve_compact_drs_using_dataguids(
                identifier, object_id, resolver_hostname="https://dataguids_mock.org"
            )
            expected = "test.commons1.io"
            assert results == expected

            object_id = "dg.TEST/00e6cfa9-a183-42f6-bb44-b70347106bbe"
            identifier = "dg.TEST"
            m.get(
                f"https://dataguids_mock.org/index/{object_id}",
                json={},
                status_code=404,
            )
            m.get(
                f"https://dataguids_mock.org/mds/metadata/{identifier}",
                json=DRS_MDS_RESULTS,
            )
            results = resolve_compact_drs_using_dataguids(
                identifier, object_id, resolver_hostname="https://dataguids_mock.org"
            )
            expected = "test.commons1.io"
            assert results == expected
            m.get(
                f"https://dataguids_mock.org/index/{object_id}",
                json={},
                status_code=500,
            )

            m.get(
                f"https://dataguids_mock.org/mds/metadata/{identifier}",
                json=DRS_MDS_RESULTS,
            )
            assert (
                resolve_compact_drs_using_dataguids(
                    identifier,
                    object_id,
                    resolver_hostname="https://dataguids_mock.org",
                )
                == None
            )

            m.get(
                f"https://dataguids_mock.org/mds/metadata/{identifier}",
                json=DRS_MDS_RESULTS,
                status_code=404,
            )
            assert (
                resolve_compact_drs_using_dataguids(
                    identifier,
                    object_id,
                    resolver_hostname="https://dataguids_mock.org",
                )
                is None
            )

            object_id = "dg.TEST/00e6cfa9-a183-42f6-bb44-b70347106bbe"
            identifier = "dg.TEST"
            m.get(
                f"https://dataguids_mock.org/index/{object_id}",
                json={},
                status_code=404,
            )
            m.get(
                f"https://dataguids_mock.org/mds/metadata/{identifier}",
                json=DRS_MDS_RESULTS,
                status_code=500,
            )
            assert (
                resolve_compact_drs_using_dataguids(
                    identifier,
                    object_id,
                    resolver_hostname="https://dataguids_mock.org",
                )
                is None
            )


@pytest.mark.parametrize(
    "identifier,expected",
    [
        ("dg.4503", "gen3.biodatacatalyst.nhlbi.nih.gov"),
        ("dg.6VTS", "jcoin.datacommons.io"),
        ("dg.5656", None),
    ],
)
def test_resolve_using_mds(download_dir, identifier, expected):
    MDS_RESPONSE = {
        "dg.4503": {
            "host": "https://gen3.biodatacatalyst.nhlbi.nih.gov/index/",
            "name": "DataSTAGE",
            "type": "indexd",
        },
        "dg.6VTS": {
            "host": "https://jcoin.datacommons.io/ga4gh/drs/v1/objects/",
            "name": "JCOIN",
            "type": "DRS",
        },
    }

    with mock.patch(
        "gen3.tools.download.drs_resolvers.DRS_CACHE",
        str(Path(download_dir, ".drs_cache", "resolved_drs_hosts.json")),
    ):
        with requests_mock.Mocker() as m:
            m.get(
                f"https://dataguids_mock.org/mds/metadata/{identifier}",
                json=MDS_RESPONSE.get(identifier, {"detail": "Not found: dg.4503444"}),
                status_code=200 if identifier in MDS_RESPONSE else 404,
            )

            results = resolve_drs_using_commons_mds(
                identifier,
                None,
                metadata_service_url="https://dataguids_mock.org/mds/metadata",
            )
            assert expected == results


@pytest.mark.parametrize(
    "identifier,object_id, expected",
    [
        ("dg.F82A1A", None, "data.kidsfirstdrc.org"),
        ("dg.ANV0", None, "gen3.theanvil.io"),
        (
            "dg.XXTS",
            "dg.XXTS/b96018c5-db06-4af8-a195-28e339ba815e",
            "test1.testcommons1.org",
        ),
    ],
)
def test_resolve_drs_strategy(download_dir, identifier, object_id, expected):
    DATAGUIDS_MDS_RESPONSE = {
        "dg.F82A1A": {
            "host": "https://data.kidsfirstdrc.org/index/",
            "name": "DataSTAGE",
            "type": "indexd",
        },
        "dg.XXTS": {
            "host": "https://test.commons1.io/index/",
            "name": "TestCommons",
            "type": "indexd",
        },
        "dg.ANV0": {
            "host": "https://gen3.theanvil.io/index/",
            "name": "DataSTAGE",
            "type": "indexd",
        },
        "dg.4503": {
            "host": "https://gen3.biodatacatalyst.nhlbi.nih.gov/index/",
            "name": "DataSTAGE",
            "type": "indexd",
        },
        "dg.6VTS": {
            "host": "https://jcoin.datacommons.io/ga4gh/drs/v1/objects/",
            "name": "JCOIN",
            "type": "DRS",
        },
    }

    with mock.patch(
        "gen3.tools.download.drs_resolvers.DRS_CACHE",
        str(Path(download_dir, ".drs_cache", "resolved_drs_hosts.json")),
    ):
        with requests_mock.Mocker() as m:
            # mock every request
            if object_id is not None:
                m.get(
                    f"https://dataguids_mock.org/index/{object_id}",
                    json=DATAGUIDS_MDS_RESPONSE[identifier],
                )
            m.get(
                f"https://origin.mock.org/mds/metadata/{identifier}",
                json=DATAGUIDS_MDS_RESPONSE[identifier],
            )

            results = resolve_drs(
                identifier,
                object_id,
                metadata_service_url="https://origin.mock.org/mds/metadata",
                resolver_hostname="https://dataguids_mock.org",
                cache_results=True,
            )
            assert results == expected


def test_missing_resolvers():
    assert resolve_drs_via_list([], "dg.XXTS", None) is None
