import pytest
from requests import HTTPError

# helper functions -------------------------------------------------
def get_rec(indexd_client, guid):
    # testing get_record
    return indexd_client.get_record(guid)


# tests ------------------------------------------------------------


def testsystem(indexd_client):
    """ Test that indexd_client is healthy 
    """
    assert indexd_client.get_status().status_code == 200
    assert indexd_client.get_version()
    assert indexd_client.get_stats()
    assert indexd_client.get_index()


# -------------------------------------------------------------------


def testget_urls(indexd_client):
    """ Test get_urls
    """
    indexd_client.add_record(hashes={"md5": "374c12456782738abcfe387492837483"}, size=0)
    # put a new record in the index
    indexd_client.add_record(hashes={"md5": "adbc12447582738abcfe387492837483"}, size=2)
    # put a new record in the index
    indexd_client.add_record(hashes={"md5": "adbc82746782738abcfe387492837483"}, size=1)

    assert indexd_client.get_urls(hashes="md5:374c12456782738abcfe387492837483")
    assert indexd_client.get_urls(size=1)
    assert indexd_client.get_urls(size=2)


# -------------------------------------------------------------------


def testbulk(indexd_client):
    """ Test get_record_bulk
    """
    # put a new record in the index
    rec1 = indexd_client.add_record(
        hashes={"md5": "374c12456782738abcfe387492837483"}, size=0
    )
    # put a new record in the index
    rec2 = indexd_client.add_record(
        hashes={"md5": "adbc12447582738abcfe387492837483"}, size=0
    )
    # put a new record in the index
    rec3 = indexd_client.add_record(
        hashes={"md5": "adbc82746782738abcfe387492837483"}, size=0
    )
    recs = indexd_client.get_record_bulk([rec1["did"], rec2["did"], rec3["did"]])

    dids = [rec1["did"]] + [rec2["did"]] + [rec3["did"]]
    v = True
    for rec in recs:
        if rec["did"] not in dids:
            v = False

    assert v


# -------------------------------------------------------------------


def test_getwithparams(indexd_client):
    """ test get_with_params
    """
    # put a new record in the index
    rec1 = indexd_client.add_record(
        hashes={"md5": "374c12456782738abcfe387492837483"}, size=0
    )
    # put a new record in the index
    rec2 = indexd_client.add_record(
        hashes={"md5": "adbc12447582738abcfe387492837483"}, size=1
    )
    # put a new record in the index
    rec3 = indexd_client.add_record(
        hashes={"md5": "adbc82746782738abcfe387492837483"}, size=2
    )
    check1 = indexd_client.get_with_params({"size": rec1["size"]})
    assert rec1["did"] == check1["did"]
    check2 = indexd_client.get_with_params({"hashes": rec2["hashes"]})
    assert rec2["did"] == check2["did"]
    check3 = indexd_client.get_with_params(
        {"size": rec3["size"], "hashes": rec3["hashes"]}
    )
    assert rec3["did"] == check3["did"]


# -------------------------------------------------------------------


def testnewrecord(indexd_client):
    """ Test the creation, update, and deletion a record

        index.py functions tested:
            add_record 
            global_get
            get_record
            update_record
            delete_record
    """

    # put a new record in the index
    newrec = indexd_client.add_record(
        hashes={"md5": "adbc12456782738abcfe387492837483"}, size=0
    )
    # testing global get
    checkrec = indexd_client.global_get(newrec["baseid"])
    assert (
        newrec["did"] == checkrec["did"]
        and newrec["baseid"] == checkrec["baseid"]
        and newrec["rev"] == checkrec["rev"]
    )

    # update the record
    updated = indexd_client.update_record(
        newrec["did"], acl=["prog1", "proj1"], file_name="fakefilename"
    )
    updatedrec = get_rec(indexd_client, updated["did"])
    # Note: I am not sure why the program and project are flipped!!
    assert updatedrec["acl"] == ["prog1", "proj1"]
    assert updatedrec["file_name"] == "fakefilename"
    assert updatedrec["did"] == checkrec["did"]
    assert updatedrec["rev"] != checkrec["rev"]

    # delete the record
    drec = indexd_client.delete_record(updatedrec["did"])
    assert drec._deleted


# -------------------------------------------------------------------


def testversions(indexd_client):
    """ Test creation of a record and a new version of it

        index.py functions tested:
            add_record
            add_new_version
            get_versions
            get_latestversion
    """
    # put a new record in the index
    newrec = indexd_client.add_record(
        acl=["prog1", "proj1"],
        hashes={"md5": "437283456782738abcfe387492837483"},
        size=0,
        version="1",
    )

    # update the record
    newversion = indexd_client.add_new_version(
        newrec["did"],
        acl=["prog1", "proj1"],
        hashes={"md5": "437283456782738abcfe387492837483"},
        size=1,
        version="2",
    )

    newrec = get_rec(indexd_client, newrec["did"])
    newversion = get_rec(indexd_client, newversion["did"])

    assert newrec["did"] != newversion["did"]
    assert newrec["baseid"] == newversion["baseid"]

    #   These functions do not recognize the records for some reason!
    versions = indexd_client.get_versions(newversion["did"])
    latestversion = indexd_client.get_latestversion(newrec["did"], "false")

    assert versions[0]["did"] == newrec["did"]
    assert versions[1]["did"] == newversion["did"]

    assert latestversion["did"] == newversion["did"]
    assert latestversion["version"] == "2"


# -------------------------------------------------------------------

# the endpoint /blank is having some sort of authorization problem
# it asks for username and password even when given auth file
def testblank(indexd_client):
    """ Test create and update blank record
    """
    newblank = indexd_client.create_blank("mjmartinson")
    checkrec = get_rec(indexd_client, newblank["did"])
    assert (
        newblank["did"] == checkrec["did"]
        and newblank["baseid"] == checkrec["baseid"]
        and newblank["rev"] == checkrec["rev"]
    )

    # update the record
    updated = indexd_client.update_blank(
        newblank["did"],
        newblank["rev"],
        hashes={"md5": "4372834515237483626e387492837483"},
        size=1,
    )

    updatedblank = get_rec(indexd_client, updated["did"])
    assert updatedblank["did"] == checkrec["did"]
    assert updatedblank["size"] == 1
    assert updatedblank["hashes"] == {"md5": "4372834515237483626e387492837483"}
    assert updatedblank["rev"] != checkrec["rev"]

    # delete the record
    drec = indexd_client.delete_record(updatedblank["did"])
    assert drec._deleted


# -------------------------------------------------------------------
