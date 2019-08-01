import pytest
from requests import HTTPError

# helper functions -------------------------------------------------
def get_rec(indexd, guid):
    # testing get_record
    return indexd.get_record(guid).json()


def no_record(indexd, guid):
    try:
        get_rec(indexd, guid)

        return False
    except HTTPError as error:
        if "no record found" in str(error):
            return True
        else:
            raise "Something is wrong!"


def same_recs(rec1, rec2):
    rec1.pop("rev", None)
    rec1.pop("version", None)
    rec2.pop("rev", None)
    rec2.pop("version", None)
    print(rec1, "\n", rec2)
    if rec1 == rec2:
        return True
    else:
        return False


# tests ------------------------------------------------------------


def testsystem(indexd):
    """ Test that indexd is healthy 
    """
    assert indexd.get_status()[0].status_code == 200
    assert indexd.get_version().status_code == 200
    assert indexd.get_stats().status_code == 200
    assert indexd.get_index().status_code == 200


# -------------------------------------------------------------------


def testbulk(indexd):
    """ Test get_record_bulk
    """
    # put a new record in the index
    rec1 = indexd.add_record(
        {
            "authz": ["/programs/prog1/projects/proj1"],
            "hashes": {"md5": "374c12456782738abcfe387492837483"},
            "size": 0,
        }
    ).json()
    # put a new record in the index
    rec2 = indexd.add_record(
        {
            "authz": ["/programs/prog1/projects/proj1"],
            "hashes": {"md5": "adbc12447582738abcfe387492837483"},
            "size": 0,
        }
    ).json()
    # put a new record in the index
    rec3 = indexd.add_record(
        {
            "authz": ["/programs/prog1/projects/proj1"],
            "hashes": {"md5": "adbc82746782738abcfe387492837483"},
            "size": 0,
        }
    ).json()

    recs = indexd.get_record_bulk([rec1["did"], rec2["did"], rec3["did"]])

    dids = [rec1["did"]] + [rec2["did"]] + [rec3["did"]]
    v = True
    for rec in recs.json():
        if rec["did"] not in dids:
            v = False

    assert v


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
        {
            "authz": ["/programs/prog1/projects/proj1"],
            "hashes": {"md5": "adbc12456782738abcfe387492837483"},
            "size": 0,
        }
    ).json()

    # testing global get
    checkrec = indexd.global_get(newrec["did"]).json()
    assert (
        newrec["did"] == checkrec["did"]
        and newrec["baseid"] == checkrec["baseid"]
        and newrec["rev"] == checkrec["rev"]
    )

    # update the record
    updated = indexd.update_record(
        newrec["did"],
        newrec["rev"],
        {
            "acl": ["prog1", "proj1"],
            "file_name": "fakefilename",
            "uploader": "mjmartinson",
        },
    ).json()

    updatedrec = get_rec(indexd, updated["did"])
    # Note: I am not sure why the program and project are flipped!!
    assert (
        updatedrec["acl"] == ["proj1", "prog1"]
        and updatedrec["file_name"] == "fakefilename"
        and updatedrec["uploader"] == "mjmartinson"
        and updatedrec["did"] == checkrec["did"]
        and updatedrec["rev"] != checkrec["rev"]
    )

    # delete the record
    response = indexd.delete_record(updatedrec["did"], updatedrec["rev"])
    assert response.status_code == 200 and no_record(indexd, updatedrec["did"]) == True


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
        {
            "acl": ["prog1", "proj1"],
            "authz": ["/programs/prog1/projects/proj1"],
            "hashes": {"md5": "437283456782738abcfe387492837483"},
            "size": 0,
            "version": "1",
            "uploader": "mjmartinson",
        }
    ).json()

    # update the record
    newversion = indexd.add_new_version(
        newrec["did"],
        {
            "acl": ["prog1", "proj1"],
            "authz": ["/programs/prog1/projects/proj1"],
            "hashes": {"md5": "437283456782738abcfe387492837483"},
            "size": 1,
            "version": "2",
            "uploader": "mjmartinson",
        },
    ).json()

    assert get_rec(indexd, newrec["did"])
    assert get_rec(indexd, newversion["did"])

    print(newrec)
    print(newversion)

    #   These functions do not recognize the records for some reason!
    versions = indexd.get_versions(newversion["did"])
    latestversion = indexd.get_latestversion(newrec["did"], "false")


# -------------------------------------------------------------------

# the endpoint /blank is having some sort of authorization problem
# it asks for username and password even when given auth file
@pytest.mark.skip
def testblank(indexd):
    """ Test create and update blank record
     """
    newblank = indexd.create_blank({"uploader": "mjmartinson"})

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
        {"size": 1, "hashes": {"md5": "4372834515237483626e387492837483"}},
    ).json()

    updatedblank = get_rec(indexd, updated["did"])
    assert (
        updatedblank["did"] == checkrec["did"]
        and updatedblank["size"] == 1
        and updatedblank["did"] == {"md5": "4372834515237483626e387492837483"}
        and updatedblank["rev"] != checkrec["rev"]
    )

    # delete the record
    response = indexd.delete_record(updatedblank["did"], updatedblank["rev"])
    assert (
        response.status_code == 200 and no_record(indexd, updatedblank["did"]) == True
    )


# -------------------------------------------------------------------

""" Functions that Do not work right now 7/25

    get_urls - do not understand input parameters
    get_latestversion
    get_versions

    No JWT auth?
    create_blank
    update_blank
"""
