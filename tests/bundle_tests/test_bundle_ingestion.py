import os
import sys
import logging
from unittest.mock import MagicMock, patch
from drsclient.client import DrsClient

from gen3.tools.bundle.ingest_manifest import (
    _replace_bundle_name_with_guid,
    ingest_bundle_manifest,
)


def test_replace_bundle_name_with_guid():
    list_with_guid_name = [
        "bundleA",
        "bundleB",
        "05b30df8-20f4-4f61-b860-902ddb9ddf0b",
        "dg.xxxx/05b30df8-20f4-4f61-b860-902ddb9ddf0b",
    ]
    expected_list = [
        "51eea3c7-cbbb-4d32-9045-c626f302e1ef",
        "ba989365-43a3-4fa8-9360-5cdb7ef0179c",
        "05b30df8-20f4-4f61-b860-902ddb9ddf0b",
        "dg.xxxx/05b30df8-20f4-4f61-b860-902ddb9ddf0b",
    ]
    MAIN_DICT = {
        "bundleA": "51eea3c7-cbbb-4d32-9045-c626f302e1ef",
        "bundleB": "ba989365-43a3-4fa8-9360-5cdb7ef0179c",
    }
    list_with_guid = _replace_bundle_name_with_guid(list_with_guid_name, MAIN_DICT)
    assert list_with_guid == expected_list


def test_valid_ingest_bundle_manifest(gen3_index, indexd_server, drs_client):
    """
    Test valid manifest
    """
    # Create some indexd records to bundle
    rec1 = gen3_index.create_record(
        did="dg.TEST/f2a39f98-6ae1-48a5-8d48-825a0c52a22b",
        hashes={"md5": "a1234567891234567890123456789012"},
        size=123,
        acl=["DEV", "test"],
        authz=["/programs/DEV/projects/test"],
        urls=["s3://testaws/aws/test.txt", "gs://test/test.txt"],
    )
    rec2 = gen3_index.create_record(
        did="dg.TEST/1e9d3103-cbe2-4c39-917c-b3abad4750d2",
        hashes={"md5": "b1234567891234567890123456789012"},
        size=234,
        acl=["DEV", "test2"],
        authz=["/programs/DEV/projects/test2", "/programs/DEV/projects/test2bak"],
        urls=["gs://test/test.txt"],
        file_name="test.txt",
    )
    rec3 = gen3_index.create_record(
        did="dg.TEST/ed8f4658-6acd-4f96-9dd8-3709890c959e",
        hashes={"md5": "e1234567891234567890123456789012"},
        size=345,
        acl=["DEV", "test3"],
        authz=["/programs/DEV/projects/test3", "/programs/DEV/projects/test3bak"],
        urls=["gs://test/test3.txt"],
    )

    records = ingest_bundle_manifest(
        indexd_server.baseurl,
        "./tests/bundle_tests/valid_manifest.csv",
        manifest_file_delimiter=",",
        auth=("user", "user"),
    )

    # 5 bundles in the manfiest
    assert len(records) == 5

    resp = drs_client.get("dg.xxxx/590ee63d-2790-477a-bbf8-d53873ca4933")
    assert resp.status_code == 200

    resp1 = drs_client.get_all(endpoint="/bundle")
    assert resp1.status_code == 200
    res1 = resp1.json()
    assert len(res1["records"]) == 5

    for record in res1["records"]:
        assert record["name"] in ["A", "B", "C", "D", "E"]


def test_invalid_ingest_bundle_manifest(gen3_index, indexd_server, drs_client):
    """
    Test invalid manifest
    """
    # Create some indexd records to bundle
    rec1 = gen3_index.create_record(
        did="dg.TEST/f2a39f98-6ae1-48a5-8d48-825a0c52a22b",
        hashes={"md5": "a1234567891234567890123456789012"},
        size=123,
        acl=["DEV", "test"],
        authz=["/programs/DEV/projects/test"],
        urls=["s3://testaws/aws/test.txt", "gs://test/test.txt"],
    )
    rec2 = gen3_index.create_record(
        did="dg.TEST/1e9d3103-cbe2-4c39-917c-b3abad4750d2",
        hashes={"md5": "b1234567891234567890123456789012"},
        size=234,
        acl=["DEV", "test2"],
        authz=["/programs/DEV/projects/test2", "/programs/DEV/projects/test2bak"],
        urls=["gs://test/test.txt"],
        file_name="test.txt",
    )
    rec3 = gen3_index.create_record(
        did="dg.TEST/ed8f4658-6acd-4f96-9dd8-3709890c959e",
        hashes={"md5": "e1234567891234567890123456789012"},
        size=345,
        acl=["DEV", "test3"],
        authz=["/programs/DEV/projects/test3", "/programs/DEV/projects/test3bak"],
        urls=["gs://test/test3.txt"],
    )

    records = ingest_bundle_manifest(
        indexd_server.baseurl,
        "./tests/bundle_tests/invalid_manifest.csv",
        manifest_file_delimiter=",",
        auth=("user", "user"),
    )

    assert records == None

    resp = drs_client.get("dg.xxxx/590ee63d-2790-477a-bbf8-d53873ca4933")
    assert resp.status_code == 404

    resp1 = drs_client.get_all(endpoint="/bundle")
    rec1 = resp1.json()
    assert len(rec1["records"]) == 0
