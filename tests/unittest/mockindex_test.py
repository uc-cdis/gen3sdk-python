import pytest
from requests import HTTPError

# helper functions -------------------------------------------------
def get_rec(indexd_client, guid):
    # testing get_record
    return indexd_client.get_record(guid)


def no_record(indexd_client, guid):
    try:
        get_rec(indexd_client, guid)

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


def testsystem(indexd_client):
    """ Test that indexd_client is healthy 
    """
    assert indexd_client.get_status()[0].status_code == 200
    assert indexd_client.get_version()
    assert indexd_client.get_stats()
    assert indexd_client.get_index()


# -------------------------------------------------------------------


def testbulk(indexd_client):
    """ Test get_record_bulk
    """
    # put a new record in the index
    rec1 = indexd_client.add_record(
        authz=["/programs/prog1/projects/proj1"],
        hashes={"md5": "374c12456782738abcfe387492837483"},
        size=0,
    )
    # put a new record in the index
    rec2 = indexd_client.add_record(
        authz=["/programs/prog1/projects/proj1"],
        hashes={"md5": "adbc12447582738abcfe387492837483"},
        size=0,
    )
    # put a new record in the index
    rec3 = indexd_client.add_record(
        authz=["/programs/prog1/projects/proj1"],
        hashes={"md5": "adbc82746782738abcfe387492837483"},
        size=0,
    )
    recs = indexd_client.get_record_bulk(
            [rec1["did"], rec2["did"], rec3["did"]])

    dids = [rec1["did"]] + [rec2["did"]] + [rec3["did"]]
    v = True
    for rec in recs:
        if rec["did"] not in dids:
            v = False

    assert v


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
            authz = ["/programs/prog1/projects/proj1"],
            hashes = {"md5": "adbc12456782738abcfe387492837483"},
            size = 0,
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
        newrec["did"],
            acl = ["prog1", "proj1"],
            file_name = "fakefilename",
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
@pytest.mark.skip
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
    newversion = indexd_client.add_new_version(
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

    assert get_rec(indexd_client, newrec["did"])
    assert get_rec(indexd_client, newversion["did"])

    print(newrec)
    print(newversion)

    #   These functions do not recognize the records for some reason!
    versions = indexd_client.get_versions(newversion["did"])
    latestversion = indexd_client.get_latestversion(newrec["did"], "false")


# -------------------------------------------------------------------

# the endpoint /blank is having some sort of authorization problem
# it asks for username and password even when given auth file
@pytest.mark.skip
def testblank(indexd_client):
    """ Test create and update blank record
     """
    newblank = indexd_client.create_blank({"uploader": "mjmartinson"})

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
        {"size": 1, "hashes": {"md5": "4372834515237483626e387492837483"}},
    ).json()

    updatedblank = get_rec(indexd_client, updated["did"])
    assert (
        updatedblank["did"] == checkrec["did"]
        and updatedblank["size"] == 1
        and updatedblank["did"] == {"md5": "4372834515237483626e387492837483"}
        and updatedblank["rev"] != checkrec["rev"]
    )

    # delete the record
    response = indexd_client.delete_record(updatedblank["did"], updatedblank["rev"])
    assert (
        response.status_code == 200
        and no_record(indexd_client, updatedblank["did"]) == True
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
