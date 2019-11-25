import pytest, os, requests
from unittest.mock import patch


def test_get(sub):
    """ 
        tests: 
        get_programs
        get_graphql_schema
        get_dictionary_all

    """
    with patch("gen3.submission.requests") as mock_request:
        mock_request.status_code = 200
        mock_request.get().text = '{ "key": "value" }'
        assert sub.get_programs()
        try:
            sub.get_graphql_schema()
        except:
            assert False
        try:
            sub.get_dictionary_all()
        except:
            assert False


def test_exportnode(sub):
    """ 
        tests:
        export_node

    """
    with patch("gen3.submission.requests") as mock_request:
        mock_request.status_code = 200
        mock_request.get().text = '{ "key": "value" }'
        resp = sub.export_node("DEV", "test", "experiment", "json", "node_file.json")
        assert os.path.exists("node_file.json")
        os.remove("node_file.json")
        assert not os.path.exists("node_file.json")


def test_create_program(sub):

    with patch("gen3.submission.requests") as mock_request:
        mock_request.status_code = 200
        mock_request.json.return_value = '{ "key": "value" }'
        p = sub.create_program(
            {
                "dbgap_accession_number": "programmjm",
                "name": "programmjm",
                "type": "program",
            }
        )
        assert p


def test_delete_program(sub):

    with patch("gen3.submission.requests") as mock_request:
        mock_request.status_code = 200
        mock_request.json.return_value = '{ "key": "value" }'
        sub.delete_program("programmjm")


def test_create_project(sub):

    with patch("gen3.submission.requests") as mock_request:
        mock_request.status_code = 200
        mock_request.json.return_value = '{ "key": "value" }'
        pj = sub.create_project(
            "programmjm",
            {
                "code": "projectmjm",
                "dbgap_accession_number": "projectmjm",
                "name": "projectmjm",
                "availability_type": "Open",
            },
        )
        assert pj
        assert sub.get_projects("programmjm")
        assert sub.get_project_dictionary("programmjm", "projectmjm")


def test_delete_project(sub):

    with patch("gen3.submission.requests") as mock_request:
        mock_request.status_code = 200
        mock_request.json.return_value = '{ "key": "value" }'
        dpj = sub.delete_project("programmjm", "projectmjm")


def test_open_project(sub):
    with patch("gen3.submission.requests") as mock_request:
        mock_request.status_code = 200
        mock_request.json.return_value = '{ "key": "value" }'
        assert sub.open_project("programmjm", "projectmjm")


def test_submit_record(sub):
    with patch("gen3.submission.requests") as mock_request:
        mock_request.status_code = 200
        mock_request.json.return_value = '{ "key": "value" }'
        rec = sub.submit_record(
            "prog1",
            "proj1",
            {
                "projects": [{"code": "proj1"}],
                "submitter_id": "mjmartinson",
                "type": "experiment",
            },
        )
        assert rec


def test_export_record(sub):
    with patch("gen3.submission.requests") as mock_request:
        mock_request.status_code = 200
        mock_request.get().text = '{ "key": "value" }'
        sub.export_record("prog1", "proj1", "id", "json", "record_file.json")
        assert os.path.exists("record_file.json")
        os.remove("record_file.json")
        assert not os.path.exists("record_file.json")


def test_delete_record(sub):
    with patch("gen3.submission.requests") as mock_request:
        mock_request.status_code = 200
        mock_request.json.return_value = '{ "key": "value" }'
        sub.delete_record("prog1", "proj1", "id")


def test_query(sub):
    with patch("gen3.submission.requests") as mock_request:
        mock_request.status_code = 200
        mock_request.post().text = '{ "key": "value" }'
        res = sub.query("{ experiment { submitter_id } }")
        assert res == {"key": "value"}


""" Not tested:

    - query : more tests
    - get_project_manifest: lack of swagger documentation. 
      error about not submitting ids
    - submit_file

"""
