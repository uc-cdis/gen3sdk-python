import asyncio
import os
import glob
import sys
import shutil
import logging
from unittest.mock import MagicMock, patch

from gen3.tools.indexing import async_verify_object_manifest
from gen3.tools.indexing import download_manifest
from gen3.tools.indexing.download_manifest import TMP_FOLDER
from gen3.tools.indexing import async_download_object_manifest
from gen3.tools.indexing.index_manifest import (
    index_object_manifest,
    get_and_verify_fileinfos_from_tsv_manifest,
)


CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))


@patch("gen3.tools.indexing.verify_manifest.Gen3Index")
def test_verify_manifest(mock_index):
    """
    Test that verify manifest function correctly writes out log file
    with expected error information.

    NOTE: records in indexd are mocked
    """
    mock_index.return_value.async_get_record.side_effect = _async_mock_get_guid
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    loop.run_until_complete(
        async_verify_object_manifest(
            "http://localhost",
            manifest_file=CURRENT_DIR + "/test_manifest.csv",
            max_concurrent_requests=3,
            output_filename="test.log",
        )
    )

    logs = {}
    try:
        with open("test.log") as file:
            for line in file:
                guid, error, expected, actual = line.strip("\n").split("|")
                logs.setdefault(guid, {})[error] = {
                    "expected": expected.split("expected ")[1],
                    "actual": actual.split("actual ")[1],
                }
    except Exception as exc:
        # unexpected file format, fail test
        assert False

    # everything in indexd is mocked to be correct for this one
    assert "dg.TEST/f2a39f98-6ae1-48a5-8d48-825a0c52a22b" not in logs

    assert "dg.TEST/1e9d3103-cbe2-4c39-917c-b3abad4750d2" in logs
    assert "dg.TEST/9c205cd7-c399-4503-9f49-5647188bde66" in logs

    # ensure logs exist for fields that are mocked to be incorrect in indexd
    assert "/programs/DEV/projects/test2" in logs[
        "dg.TEST/1e9d3103-cbe2-4c39-917c-b3abad4750d2"
    ].get("authz", {}).get("expected")
    assert "DEV" in logs["dg.TEST/1e9d3103-cbe2-4c39-917c-b3abad4750d2"].get(
        "acl", {}
    ).get("expected")
    assert "235" in logs["dg.TEST/1e9d3103-cbe2-4c39-917c-b3abad4750d2"].get(
        "file_size", {}
    ).get("expected")
    assert "c1234567891234567890123456789012" in logs[
        "dg.TEST/1e9d3103-cbe2-4c39-917c-b3abad4750d2"
    ].get("md5", {}).get("expected")
    assert "gs://test/test3.txt" in logs[
        "dg.TEST/1e9d3103-cbe2-4c39-917c-b3abad4750d2"
    ].get("urls", {}).get("expected")

    # make sure error exists when record doesnt exist in indexd
    assert "no_record" in logs["dg.TEST/9c205cd7-c399-4503-9f49-5647188bde66"]


def test_download_manifest(monkeypatch, gen3_index):
    """
    Test that dowload manifest generates a file with expected content.
    """
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
    # record with space
    rec4 = gen3_index.create_record(
        did="dg.TEST/a802e27d-4a5b-42e3-92b0-ba19e81b9dce",
        hashes={"md5": "f1234567891234567890123456789012"},
        size=345,
        acl=["DEV", "test4"],
        authz=["/programs/DEV/projects/test4", "/programs/DEV/projects/test4bak"],
        urls=["gs://test/test4 space.txt", "s3://test/test4 space.txt"],
    )
    # mock_index.return_value.get_stats.return_value = gen3_index.get("/_stats")

    monkeypatch.setattr(download_manifest, "INDEXD_RECORD_PAGE_SIZE", 2)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    loop.run_until_complete(
        async_download_object_manifest(
            "http://localhost:8001",
            output_filename="object-manifest.csv",
            num_processes=1,
        )
    )

    records = {}
    try:
        with open("object-manifest.csv") as file:
            # skip header
            next(file)
            for line in file:
                guid, urls, authz, acl, md5, file_size, file_name = line.split(",")
                guid = guid.strip("\n")
                urls = urls.split(" ")
                authz = authz.split(" ")
                acl = acl.split(" ")
                file_size = file_size.strip("\n")
                file_name = file_name.strip("\n")

                records[guid] = {
                    "urls": urls,
                    "authz": authz,
                    "acl": acl,
                    "md5": md5,
                    "file_size": file_size,
                    "file_name": file_name,
                }
    except Exception:
        # unexpected file format, fail test
        assert False

    # ensure downloaded manifest populates expected info for a record
    assert "gs://test/test.txt" in records.get(
        "dg.TEST/f2a39f98-6ae1-48a5-8d48-825a0c52a22b", {}
    ).get("urls", [])
    assert "s3://testaws/aws/test.txt" in records.get(
        "dg.TEST/f2a39f98-6ae1-48a5-8d48-825a0c52a22b", {}
    ).get("urls", [])
    assert "/programs/DEV/projects/test" in records.get(
        "dg.TEST/f2a39f98-6ae1-48a5-8d48-825a0c52a22b", {}
    ).get("authz", [])
    assert "DEV" in records.get("dg.TEST/f2a39f98-6ae1-48a5-8d48-825a0c52a22b", {}).get(
        "acl", []
    )
    assert "test" in records.get(
        "dg.TEST/f2a39f98-6ae1-48a5-8d48-825a0c52a22b", {}
    ).get("acl", [])
    assert "123" in records.get("dg.TEST/f2a39f98-6ae1-48a5-8d48-825a0c52a22b", {}).get(
        "file_size"
    )
    assert "a1234567891234567890123456789012" in records.get(
        "dg.TEST/f2a39f98-6ae1-48a5-8d48-825a0c52a22b", {}
    ).get("md5")
    assert not records.get("dg.TEST/f2a39f98-6ae1-48a5-8d48-825a0c52a22b", {}).get(
        "file_name"
    )
    assert "gs://test/test4%20space.txt" in records.get(
        "dg.TEST/a802e27d-4a5b-42e3-92b0-ba19e81b9dce", {}
    ).get("urls", [])
    assert "s3://test/test4%20space.txt" in records.get(
        "dg.TEST/a802e27d-4a5b-42e3-92b0-ba19e81b9dce", {}
    ).get("urls", [])

    # assert other 2 records exist
    assert "dg.TEST/ed8f4658-6acd-4f96-9dd8-3709890c959e" in records
    assert "dg.TEST/1e9d3103-cbe2-4c39-917c-b3abad4750d2" in records
    assert "test.txt" == records.get(
        "dg.TEST/1e9d3103-cbe2-4c39-917c-b3abad4750d2", {}
    ).get("file_name")


def _mock_get_guid(guid, **kwargs):
    if guid == "dg.TEST/f2a39f98-6ae1-48a5-8d48-825a0c52a22b":
        return {
            "acl": ["DEV", "test"],
            "authz": ["/programs/DEV/projects/test"],
            "baseid": f"'1' + {guid[1:-1]} + '1'",
            "created_date": "2019-11-24T18:29:48.218755",
            "did": f"{guid}",
            "file_name": None,
            "form": "object",
            "hashes": {"md5": "a1234567891234567890123456789012"},
            "metadata": {},
            "rev": "abc123",
            "size": 123,
            "updated_date": "2019-11-24T18:29:48.218761",
            "uploader": None,
            "urls": ["s3://testaws/aws/test.txt", "gs://test/test.txt"],
            "urls_metadata": {
                "gs://test/test.txt": {},
                "s3://testaws/aws/test.txt": {},
            },
            "version": None,
        }
    elif guid == "dg.TEST/1e9d3103-cbe2-4c39-917c-b3abad4750d2":
        return {
            "acl": ["DEV", "test2"],
            "authz": [
                "/programs/DEV/projects/test2",
                "/programs/DEV/projects/test2bak",
            ],
            "baseid": f"'1' + {guid[1:-1]} + '1'",
            "created_date": "2019-11-24T18:29:48.218755",
            "did": f"{guid}",
            "file_name": None,
            "form": "object",
            "hashes": {"md5": "b1234567891234567890123456789012"},
            "metadata": {},
            "rev": "abc234",
            "size": 234,
            "updated_date": "2019-11-24T18:29:48.218761",
            "uploader": None,
            "urls": ["gs://test/test.txt"],
            "urls_metadata": {"gs://test/test.txt": {}},
            "version": None,
        }
    else:
        return None


async def _async_mock_get_guid(guid, **kwargs):
    if guid == "dg.TEST/f2a39f98-6ae1-48a5-8d48-825a0c52a22b":
        return {
            "acl": ["DEV", "test"],
            "authz": ["/programs/DEV/projects/test"],
            "baseid": f"'1' + {guid[1:-1]} + '1'",
            "created_date": "2019-11-24T18:29:48.218755",
            "did": f"{guid}",
            "file_name": None,
            "form": "object",
            "hashes": {"md5": "a1234567891234567890123456789012"},
            "metadata": {},
            "rev": "abc123",
            "size": 123,
            "updated_date": "2019-11-24T18:29:48.218761",
            "uploader": None,
            "urls": ["s3://testaws/aws/test.txt", "gs://test/test.txt"],
            "urls_metadata": {
                "gs://test/test.txt": {},
                "s3://testaws/aws/test.txt": {},
            },
            "version": None,
        }
    elif guid == "dg.TEST/1e9d3103-cbe2-4c39-917c-b3abad4750d2":
        return {
            "acl": ["DEV", "test2"],
            "authz": [
                "/programs/DEV/projects/test2",
                "/programs/DEV/projects/test2bak",
            ],
            "baseid": f"'1' + {guid[1:-1]} + '1'",
            "created_date": "2019-11-24T18:29:48.218755",
            "did": f"{guid}",
            "file_name": None,
            "form": "object",
            "hashes": {"md5": "b1234567891234567890123456789012"},
            "metadata": {},
            "rev": "abc234",
            "size": 234,
            "updated_date": "2019-11-24T18:29:48.218761",
            "uploader": None,
            "urls": ["gs://test/test.txt"],
            "urls_metadata": {"gs://test/test.txt": {}},
            "version": None,
        }
    else:
        return None


def _mock_get_records_on_page(page, limit, **kwargs):
    # for testing, the limit is 2
    if page == 0:
        return [
            {
                "acl": ["DEV", "test"],
                "authz": ["/programs/DEV/projects/test"],
                "baseid": "1g.TEST/f2a39f98-6ae1-48a5-8d48-825a0c52a221",
                "created_date": "2019-11-24T18:29:48.218755",
                "did": "dg.TEST/f2a39f98-6ae1-48a5-8d48-825a0c52a22b",
                "file_name": None,
                "form": "object",
                "hashes": {"md5": "a1234567891234567890123456789012"},
                "metadata": {},
                "rev": "abc123",
                "size": 123,
                "updated_date": "2019-11-24T18:29:48.218761",
                "uploader": None,
                "urls": ["s3://testaws/aws/test.txt", "gs://test/test.txt"],
                "urls_metadata": {
                    "gs://test/test.txt": {},
                    "s3://testaws/aws/test.txt": {},
                },
                "version": None,
            },
            {
                "acl": ["DEV", "test2"],
                "authz": [
                    "/programs/DEV/projects/test2",
                    "/programs/DEV/projects/test2bak",
                ],
                "baseid": "1g.TEST/1e9d3103-cbe2-4c39-917c-b3abad4750d1",
                "created_date": "2019-11-24T18:29:48.218755",
                "did": "dg.TEST/1e9d3103-cbe2-4c39-917c-b3abad4750d2",
                "file_name": None,
                "form": "object",
                "hashes": {"md5": "b1234567891234567890123456789012"},
                "metadata": {},
                "rev": "abc234",
                "size": 234,
                "updated_date": "2019-11-24T18:29:48.218761",
                "uploader": None,
                "urls": ["gs://test/test.txt"],
                "urls_metadata": {"gs://test/test.txt": {}},
                "version": None,
            },
        ]
    elif page == 1:
        return [
            {
                "acl": ["DEV", "test3"],
                "authz": [
                    "/programs/DEV/projects/test3",
                    "/programs/DEV/projects/test3bak",
                ],
                "baseid": "1g.TEST/ed8f4658-6acd-4f96-9dd8-3709890c9591",
                "created_date": "2019-11-24T18:29:48.218755",
                "did": "dg.TEST/ed8f4658-6acd-4f96-9dd8-3709890c959e",
                "file_name": None,
                "form": "object",
                "hashes": {"md5": "e1234567891234567890123456789012"},
                "metadata": {},
                "rev": "abc345",
                "size": 345,
                "updated_date": "2019-11-24T18:29:48.218761",
                "uploader": None,
                "urls": ["gs://test/test3.txt"],
                "urls_metadata": {"gs://test/test3.txt": {}},
                "version": None,
            }
        ]
    else:
        return []


def test_read_manifest():
    files, headers = get_and_verify_fileinfos_from_tsv_manifest("./test.tsv")
    assert headers.index("guid") >= 0
    assert headers.index("md5") >= 0
    assert headers.index("urls") >= 0

    # read in as-is, all brackets and stray quotes are cleaned up later
    assert files[0]["urls"] == "[s3://pdcdatastore/test1.raw]"
    assert files[1]["acl"] == "Open"
    assert files[1]["urls"] == "s3://pdcdatastore/test2.raw"
    assert files[3]["urls"] == "['s3://pdcdatastore/test4.raw']"


def test_index_manifest(gen3_index, indexd_server):

    rec1 = gen3_index.create_record(
        did="255e396f-f1f8-11e9-9a07-0a80fada099c",
        hashes={"md5": "473d83400bc1bc9dc635e334faddf33c"},
        acl=["DEV", "test"],
        size=363_455_714,
        urls=["s3://testaws/aws/test.txt", "gs://test/test.txt"],
    )

    index_object_manifest(
        indexd_server.baseurl, "./test.tsv", 1, ("admin", "admin"), replace_urls=False
    )
    rec1 = gen3_index.get("255e396f-f1f8-11e9-9a07-0a80fada099c")
    rec2 = gen3_index.get("255e396f-f1f8-11e9-9a07-0a80fada010c")
    rec3 = gen3_index.get("255e396f-f1f8-11e9-9a07-0a80fada098c")
    rec4 = gen3_index.get("255e396f-f1f8-11e9-9a07-0a80fada097c")
    rec5 = gen3_index.get("255e396f-f1f8-11e9-9a07-0a80fada096c")
    rec6 = gen3_index.get("255e396f-f1f8-11e9-9a07-0a80fada012c")
    assert set(rec1["urls"]) == set(
        [
            "s3://testaws/aws/test.txt",
            "gs://test/test.txt",
            "s3://pdcdatastore/test1.raw",
        ]
    )

    assert rec1["authz"] == []
    assert rec2["hashes"]["md5"] == "473d83400bc1bc9dc635e334fadde33c"
    assert rec2["size"] == 363_455_714
    assert rec2["authz"] == ["/program/DEV/project/test"]
    assert rec2["urls"] == ["s3://pdcdatastore/test5.raw"]
    assert rec3["urls"] == ["s3://pdcdatastore/test2.raw"]
    assert rec3["authz"] == ["/program/DEV/project/test"]
    assert rec4["urls"] == ["s3://pdcdatastore/test3.raw"]
    assert rec4["acl"] == ["phs0001", "phs0002"]
    assert rec5["urls"] == ["s3://pdcdatastore/test4.raw"]
    assert rec5["file_name"] == "test4_file.raw"
    assert rec5["acl"] == ["phs0001", "phs0002"]
    assert rec5["authz"] == ["/program/DEV/project/test"]
    assert rec6["urls"] == ["s3://pdcdatastore/test6 space.raw"]
    assert rec6["authz"] == ["/prog ram/DEV/project/test"]


def test_index_manifest_with_replace_urls(gen3_index, indexd_server):
    rec1 = gen3_index.create_record(
        did="255e396f-f1f8-11e9-9a07-0a80fada099c",
        hashes={"md5": "473d83400bc1bc9dc635e334faddf33c"},
        acl=["DEV", "test"],
        size=363_455_714,
        urls=["s3://testaws/aws/test.txt", "gs://test/test.txt"],
    )
    index_object_manifest(
        indexd_server.baseurl, "./test.tsv", 1, ("admin", "admin"), replace_urls=True
    )
    rec1 = gen3_index.get("255e396f-f1f8-11e9-9a07-0a80fada099c")

    assert rec1["urls"] == ["s3://pdcdatastore/test1.raw"]


def test_index_non_guid_manifest(gen3_index, indexd_server):
    files, _ = index_object_manifest(
        indexd_server.baseurl, "./test2.tsv", 1, ("admin", "admin"), replace_urls=True
    )

    assert "testprefix" in files[0]["guid"]
    rec1 = gen3_index.get(files[0]["guid"])
    assert rec1["urls"] == ["s3://pdcdatastore/test1.raw"]
