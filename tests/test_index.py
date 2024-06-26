import pytest
from requests import HTTPError


def get_rec(gen3_index, guid):
    # testing get_record
    return gen3_index.get_record(guid)


def test_system(gen3_index):
    """

    Test that gen3_index is healthy

    """
    assert gen3_index.is_healthy()
    assert gen3_index.get_version()
    assert gen3_index.get_stats()


def test_get_urls(gen3_index):
    """

    Test get_urls

    """
    rec1 = gen3_index.create_record(
        hashes={"md5": "374c12456782738abcfe387492837483"}, size=0
    )
    # put a new record in the index
    rec2 = gen3_index.create_record(
        hashes={"md5": "adbc12447582738abcfe387492837483"}, size=2
    )
    # put a new record in the index
    rec3 = gen3_index.create_record(
        hashes={"md5": "adbc82746782738abcfe387492837483"}, size=1
    )

    assert gen3_index.get_urls(hashes="md5:374c12456782738abcfe387492837483")
    assert gen3_index.get_urls(size=1)
    assert gen3_index.get_urls(size=2)
    drec = gen3_index.delete_record(rec1["did"])
    assert drec._deleted
    drec = gen3_index.delete_record(rec2["did"])
    assert drec._deleted
    drec = gen3_index.delete_record(rec3["did"])
    assert drec._deleted


def test_bulk(gen3_index):
    """

    Test get_records

    """
    # put a new record in the index
    rec1 = gen3_index.create_record(
        hashes={"md5": "374c12456782738abcfe387492837483"}, size=0
    )
    # put a new record in the index
    rec2 = gen3_index.create_record(
        hashes={"md5": "adbc12447582738abcfe387492837483"}, size=0
    )
    # put a new record in the index
    rec3 = gen3_index.create_record(
        hashes={"md5": "adbc82746782738abcfe387492837483"}, size=0
    )
    recs = gen3_index.get_records([rec1["did"], rec2["did"], rec3["did"]])

    dids = [rec1["did"]] + [rec2["did"]] + [rec3["did"]]
    v = True
    for rec in recs:
        if rec["did"] not in dids:
            v = False
    assert v

    drec = gen3_index.delete_record(rec1["did"])
    assert drec._deleted
    drec = gen3_index.delete_record(rec2["did"])
    assert drec._deleted
    drec = gen3_index.delete_record(rec3["did"])
    assert drec._deleted


def test_get_with_params(gen3_index):
    """

    test get_with_params

    """
    # put a new record in the index
    rec1 = gen3_index.create_record(
        hashes={"md5": "374c12456782738abcfe387492837483"}, size=1615680
    )
    # put a new record in the index
    rec2 = gen3_index.create_record(
        hashes={"md5": "adbc82746782738abcfe387492837483"}, size=15945566
    )
    assert rec1
    assert rec2

    drec = gen3_index.delete_record(rec1["did"])
    assert drec._deleted
    drec = gen3_index.delete_record(rec2["did"])
    assert drec._deleted


def test_new_record(gen3_index):
    """

    Test the creation, update, and deletion a record

        index.py functions tested:
            create_record
            get
            get_record
            update_record
            delete_record

    """

    # put a new record in the index
    newrec = gen3_index.create_record(
        hashes={"md5": "adbc12456782738abcfe387492837483"}, size=0
    )
    # testing global get
    checkrec = gen3_index.get(newrec["baseid"])
    assert (
        newrec["did"] == checkrec["did"]
        and newrec["baseid"] == checkrec["baseid"]
        and newrec["rev"] == checkrec["rev"]
    )

    # update the record
    updated = gen3_index.update_record(
        newrec["did"], acl=["prog1", "proj1"], file_name="fakefilename"
    )
    updatedrec = get_rec(gen3_index, updated["did"])
    # Note: I am not sure why the program and project are flipped!!
    assert updatedrec["acl"] == ["prog1", "proj1"]
    assert updatedrec["file_name"] == "fakefilename"
    assert updatedrec["did"] == checkrec["did"]
    assert updatedrec["rev"] != checkrec["rev"]

    # delete the record
    drec = gen3_index.delete_record(updatedrec["did"])
    assert drec._deleted


def test_versions(gen3_index):
    """

    Test creation of a record and a new version of it

    index.py functions tested:
        create_record
        create_new_version
        get_versions
        get_latest_version

    """
    # put a new record in the index
    newrec = gen3_index.create_record(
        acl=["prog1", "proj1"],
        hashes={"md5": "437283456782738abcfe387492837483"},
        size=0,
        version="1",
    )

    # update the record
    newversion = gen3_index.create_new_version(
        newrec["did"],
        acl=["prog1", "proj1"],
        hashes={"md5": "437283456782738abcfe387492837483"},
        size=1,
        version="2",
    )

    newrec = get_rec(gen3_index, newrec["did"])
    newversion = get_rec(gen3_index, newversion["did"])

    assert newrec["did"] != newversion["did"]
    assert newrec["baseid"] == newversion["baseid"]

    #   These functions do not recognize the records for some reason!
    versions = gen3_index.get_versions(newversion["did"])
    latest_version = gen3_index.get_latest_version(newrec["did"], "false")

    assert versions[0]["did"] == newrec["did"]
    assert versions[1]["did"] == newversion["did"]

    assert latest_version["did"] == newversion["did"]
    assert latest_version["version"] == "2"

    drec = gen3_index.delete_record(newrec["did"])
    assert drec._deleted
    drec = gen3_index.delete_record(newversion["did"])
    assert drec._deleted


def test_blank(gen3_index):
    """

    Test create and update blank record

    """
    newblank = gen3_index.create_blank("mjmartinson")
    checkrec = get_rec(gen3_index, newblank["did"])
    assert (
        newblank["did"] == checkrec["did"]
        and newblank["baseid"] == checkrec["baseid"]
        and newblank["rev"] == checkrec["rev"]
    )

    # update the record
    updated = gen3_index.update_blank(
        newblank["did"],
        newblank["rev"],
        hashes={"md5": "4372834515237483626e387492837483"},
        size=1,
    )

    updatedblank = get_rec(gen3_index, updated["did"])
    assert updatedblank["did"] == checkrec["did"]
    assert updatedblank["size"] == 1
    assert updatedblank["hashes"] == {"md5": "4372834515237483626e387492837483"}
    assert updatedblank["rev"] != checkrec["rev"]

    # delete the record
    drec = gen3_index.delete_record(updatedblank["did"])
    assert drec._deleted


def test_desc_and_content_dates(gen3_index):
    """
    Tests that records can be created, updated and retrieved when description, content_created_date and content_updated_date fields are provided.
    """
    expected_description = "a description"
    expected_content_created_date = "2023-03-14T17:02:54"
    expected_content_updated_date = "2023-03-15T17:11:00"
    hashes = {"md5": "374c12456782738abcfe387492837483"}
    record = gen3_index.create_record(
        hashes=hashes,
        size=0,
        description=expected_description,
        content_created_date=expected_content_created_date,
        content_updated_date=expected_content_updated_date,
    )
    newly_created_record = gen3_index.get_record(record["did"])
    assert expected_description == newly_created_record["description"]
    assert expected_content_created_date == newly_created_record["content_created_date"]
    assert expected_content_updated_date == newly_created_record["content_updated_date"]

    expected_description = "new description"
    expected_content_created_date = "2023-04-14T17:02:54"
    expected_content_updated_date = "2023-04-15T17:11:00"
    record = gen3_index.update_record(
        record["did"],
        description=expected_description,
        content_created_date=expected_content_created_date,
        content_updated_date=expected_content_updated_date,
    )
    updated_record = gen3_index.get_record(record["did"])
    assert expected_description == updated_record["description"]
    assert expected_content_created_date == updated_record["content_created_date"]
    assert expected_content_updated_date == updated_record["content_updated_date"]

    new_version = gen3_index.create_new_version(
        updated_record["did"],
        hashes=hashes,
        size=0,
        description="new version description",
        content_created_date="2023-04-28T17:02:54",
        content_updated_date="2023-04-30T17:11:00",
    )

    version_record = gen3_index.get_record(new_version["did"])
    assert version_record["description"] == new_version["description"]
    assert version_record["content_created_date"] == new_version["content_created_date"]
    assert version_record["content_updated_date"] == new_version["content_updated_date"]

def test_create_record_response(gen3_index):
    """
    Verifies the return value for create_record echoes back the parameters for the record that was created.
    """
    to_create = {
        "hashes": {
            "md5": "374c12456782738abcfe387492837483"
        },
        "size": 10,
        "urls": ['s3://foo/bar'],
        "file_name": 'new_file',
        "acl": ['*']
    }
    record = gen3_index.create_record(**to_create)
    for field, expected_value in to_create.items():
        assert expected_value == record[field]
