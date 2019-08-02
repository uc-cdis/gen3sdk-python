import pytest
from requests import HTTPError

# helper functions -------------------------------------------------
def get_rec(indexd, guid):
    # testing get_record
    return indexd.get_record(guid)


# tests ------------------------------------------------------------


def testsystem(indexd):
    """ Test that indexd is healthy 
    """
    assert indexd.get_status()[0].status_code == 200
    assert indexd.get_version()
    assert indexd.get_stats()
    assert indexd.get_index()


# -------------------------------------------------------------------


def testget_urls(indexd):
    """ Test get_urls
    """
    rec1 = indexd.add_record(
        authz=["/programs/prog1/projects/proj1"],
        hashes={"md5": "374c12456782738abcfe387492837483"},
        size=0,
    )
    # put a new record in the index
    rec2 = indexd.add_record(
        authz=["/programs/prog1/projects/proj1"],
        hashes={"md5": "adbc12447582738abcfe387492837483"},
        size=2,
    )
    # put a new record in the index
    rec3 = indexd.add_record(
        authz=["/programs/prog1/projects/proj1"],
        hashes={"md5": "adbc82746782738abcfe387492837483"},
        size=1,
    )

    assert indexd.get_urls(hashes="md5:374c12456782738abcfe387492837483")
    assert indexd.get_urls(size=1)
    assert indexd.get_urls(size=2)

    drec = indexd.delete_record(rec1["did"])
    assert drec._deleted
    drec = indexd.delete_record(rec2["did"])
    assert drec._deleted
    drec = indexd.delete_record(rec3["did"])
    assert drec._deleted


# -------------------------------------------------------------------


def testbulk(indexd):
    """ Test get_record_bulk
    """
    # put a new record in the index
    rec1 = indexd.add_record(
        authz=["/programs/prog1/projects/proj1"],
        hashes={"md5": "374c12456782738abcfe387492837483"},
        size=0,
    )
    # put a new record in the index
    rec2 = indexd.add_record(
        authz=["/programs/prog1/projects/proj1"],
        hashes={"md5": "adbc12447582738abcfe387492837483"},
        size=0,
    )
    # put a new record in the index
    rec3 = indexd.add_record(
        authz=["/programs/prog1/projects/proj1"],
        hashes={"md5": "adbc82746782738abcfe387492837483"},
        size=0,
    )
    recs = indexd.get_record_bulk([rec1["did"], rec2["did"], rec3["did"]])

    dids = [rec1["did"]] + [rec2["did"]] + [rec3["did"]]
    v = True
    for rec in recs:
        if rec["did"] not in dids:
            v = False

    assert v

    drec = indexd.delete_record(rec1["did"])
    assert drec._deleted
    drec = indexd.delete_record(rec2["did"])
    assert drec._deleted
    drec = indexd.delete_record(rec3["did"])
    assert drec._deleted


# -------------------------------------------------------------------


def testnewrecord(indexd):
    """ Test the creation, update, and deletion a record

        index.py functions tested:
            add_record 
            global_get
            get_record
            update_record
            delete_record
    """

    # put a new record in the index
    newrec = indexd.add_record(
        authz=["/programs/prog1/projects/proj1"],
        hashes={"md5": "adbc12456782738abcfe387492837483"},
        size=0,
    )
    # testing global get
    checkrec = indexd.global_get(newrec["baseid"])
    assert (
        newrec["did"] == checkrec["did"]
        and newrec["baseid"] == checkrec["baseid"]
        and newrec["rev"] == checkrec["rev"]
    )

    # update the record
    updated = indexd.update_record(
        newrec["did"], acl=["prog1", "proj1"], file_name="fakefilename"
    )
    updatedrec = get_rec(indexd, updated["did"])
    # Note: I am not sure why the program and project are flipped!!
    assert updatedrec["acl"] == ["prog1", "proj1"]
    assert updatedrec["file_name"] == "fakefilename"
    assert updatedrec["did"] == checkrec["did"]
    assert updatedrec["rev"] != checkrec["rev"]

    # delete the record
    drec = indexd.delete_record(updatedrec["did"])
    assert drec._deleted


# -------------------------------------------------------------------


def test_getwithparams(indexd):
    """ test get_with_params
    """
    # put a new record in the index
    rec1 = indexd.add_record(
        authz=["/programs/prog1/projects/proj1"],
        hashes={"md5": "374c12456782738abcfe387492837483"},
        size=131034,
    )
    # put a new record in the index
    rec2 = indexd.add_record(
        authz=["/programs/prog1/projects/proj1"],
        hashes={"md5": "adbc12447582738abcfe387492837483"},
        size=19099099,
    )
    # put a new record in the index
    rec3 = indexd.add_record(
        authz=["/programs/prog1/projects/proj1"],
        hashes={"md5": "adbc82746782738abcfe387492837483"},
        size=28932738,
    )
    check1 = indexd.get_with_params({"size": rec1["size"]})
    assert rec1["did"] == check1["did"]
    check2 = indexd.get_with_params({"size": rec2["size"], "hashes": rec2["hashes"]})
    assert rec2["did"] == check2["did"]
    check3 = indexd.get_with_params({"size": rec3["size"], "hashes": rec3["hashes"]})
    assert rec3["did"] == check3["did"]

    drec = indexd.delete_record(rec1["did"])
    assert drec._deleted
    drec2 = indexd.delete_record(rec2["did"])
    assert drec2._deleted
    drec3 = indexd.delete_record(rec3["did"])
    assert drec3._deleted


# -------------------------------------------------------------------


@pytest.mark.skip
def testversions(indexd):
    """ Test creation of a record and a new version of it

        index.py functions tested:
            add_record
            add_new_version
            get_versions
            get_latestversion
    """
    # put a new record in the index
    newrec = indexd.add_record(
        authz=["/programs/prog1/projects/proj1"],
        acl=["prog1", "proj1"],
        hashes={"md5": "437283456782738abcfe387492837483"},
        size=0,
        version="1",
        # uploader="mjmartinson",
    )

    # update the record
    newversion = indexd.add_new_version(
        newrec["did"],
        acl=["prog1", "proj1"],
        hashes={"md5": "437283456782738abcfe387492837483"},
        size=1,
        version="2",
        # uploader= "mjmartinson"
    )

    newrec = get_rec(indexd, newrec["did"])
    newversion = get_rec(indexd, newversion["did"])

    assert newrec["did"] != newversion["did"]
    assert newrec["baseid"] == newversion["baseid"]

    #   These functions do not recognize the records for some reason!
    versions = indexd.get_versions(newversion["did"])
    latestversion = indexd.get_latestversion(newrec["did"], "false")

    assert versions[0]["did"] == newrec["did"]
    assert versions[1]["did"] == newversion["did"]

    assert latestversion["did"] == newversion["did"]
    assert latestversion["version"] == "2"
    drec = indexd.delete_record(newrec["did"])
    assert drec._deleted
    drec2 = indexd.delete_record(newversion["did"])
    assert drec2._deleted


# -------------------------------------------------------------------


@pytest.mark.skip
# the endpoint /blank is having some sort of authorization problem
# it asks for username and password even when given auth file
def testblank(indexd):
    """ Test create and update blank record
    """
    newblank = indexd.create_blank("mjmartinson")
    checkrec = get_rec(indexd, newblank["did"])
    assert (
        newblank["did"] == checkrec["did"]
        and newblank["baseid"] == checkrec["baseid"]
        and newblank["rev"] == checkrec["rev"]
    )

    # update the record
    updated = indexd.update_blank(
        newblank["did"],
        newblank["rev"],
        hashes={"md5": "4372834515237483626e387492837483"},
        size=1,
    )

    updatedblank = get_rec(indexd, updated["did"])
    assert updatedblank["did"] == checkrec["did"]
    assert updatedblank["size"] == 1
    assert updatedblank["hashes"] == {"md5": "4372834515237483626e387492837483"}
    assert updatedblank["rev"] != checkrec["rev"]

    # delete the record
    drec = indexd.delete_record(updatedblank["did"])
    assert drec._deleted


# -------------------------------------------------------------------
