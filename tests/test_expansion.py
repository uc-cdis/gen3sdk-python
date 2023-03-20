import requests, json, fnmatch, os, os.path, sys, subprocess, glob, ntpath, copy, re, operator
from os import path
from gen3.auth import Gen3Auth
from gen3.submission import Gen3Submission
from gen3.file import Gen3File
from gen3.metadata import Gen3Metadata
from gen3.tools.expansion import Gen3Expansion
import pytest
from unittest.mock import MagicMock, patch
import base64
import os
import requests
import time
import json
import gen3.auth
import shutil

tsv_header = "type\tid\tproject_id\tsubmitter_id\tage_range\tbreed\tcause_of_death\tdays_to_birth\tdays_to_death\tgender\tvital_status\tyear_of_birth\tyear_of_death\tsubjects.id\tsubjects.submitter_id\n"
tsv_result1 = "demographic\t01d08a15\tCanine-Osteosarcoma\tRW.040287\t11\tGolden Retriever\t\t\t\tfemale\t\t\t\t1228931e\tRW.040287\ndemographic\t0f787b21\tCanine-Osteosarcoma\tGH_PepRey\t7\tRottweiler\t\t\t\tfemale\t\t\t\t1228b150\tGH_PepRey\n"
tsv_result2 = "demographic\t022b52c6\tCanine-B_cell_lymphoma\tc1a2e22c\t50\t\t\t\t\tmale\t\t\t\t4f1c2a74\tSRS3133132\ndemographic\t040adea7-\tCanine-B_cell_lymphoma\tc1a32462\t10\t\t\t\t\tmale\t\t\t\t4f1d8608\tSRS3133114\n"
tsv_result3 = "diagnosis\t022b52c6\tCanine-B_cell_lymphoma\tc1a2e22c\t50\t\t\t\t\tmale\t\t\t\t4f1c2a74\tSRS3133132\ndiagnosis\t040adea7-\tCanine-B_cell_lymphoma\tc1a32462\t10\t\t\t\t\tmale\t\t\t\t4f1d8608\tSRS3133114\n"


def test_get_project_ids(mock_gen3_auth):

    endpoint = "https://test/ok"
    mock_submission = Gen3Submission(mock_gen3_auth)

    def _mock_sub_query(query_txt):
        mocked_result = {}
        if query_txt == """{project (first:0){project_id}}""":
            mocked_result = {
                "data": {
                    "project": [
                        {"project_id": "Canine-Bladder_cancer"},
                        {"project_id": "Canine-Osteosarcoma"},
                        {"project_id": "Canine-PMed_trial"},
                        {"project_id": "Canine-PMed_trial"},
                    ]
                }
            }
        elif (
            query_txt
            == """{project (first:0, with_path_to:{type:"program",name:"Canine"}){project_id}}"""
        ):
            mocked_result = {
                "data": {
                    "project": [
                        {"project_id": "Canine-Bladder_cancer"},
                        {"project_id": "Canine-PMed_trial"},
                    ]
                }
            }
        elif (
            query_txt
            == """{project (first:0, with_path_to:{type:"program",name:"Training"}){project_id}}"""
        ):
            mocked_result = {"data": {"project": [{"project_id": "Canine-Training"}]}}
        elif (
            query_txt
            == """{project (first:0, with_path_to:{type:"case",submitter_id:"case-01"}){project_id}}"""
        ):
            mocked_result = {"data": {"project": [{"project_id": "case-study-01"}]}}
        elif query_txt == """{node (first:0,of_type:"node"){project_id}}""":
            mocked_result = {
                "data": {
                    "node": [
                        {"project_id": "node-proj-01"},
                        {"project_id": "node-proj-02"},
                    ]
                }
            }

        return mocked_result

    # test name is none and (node is none or node is program)
    mock_submission.query = _mock_sub_query
    exp = Gen3Expansion(endpoint, mock_gen3_auth)
    exp.sub = mock_submission
    project_ids = exp.get_project_ids()
    assert project_ids == [
        "Canine-Bladder_cancer",
        "Canine-Osteosarcoma",
        "Canine-PMed_trial",
    ]

    project_ids = exp.get_project_ids(node="program", name="Canine")
    assert project_ids == ["Canine-Bladder_cancer", "Canine-PMed_trial"]

    project_ids = exp.get_project_ids(node="program", name=["Training", "Canine"])
    assert project_ids == [
        "Canine-Bladder_cancer",
        "Canine-PMed_trial",
        "Canine-Training",
    ]

    project_ids = exp.get_project_ids(node="node")
    assert project_ids == ["node-proj-01", "node-proj-02"]

    project_ids = exp.get_project_ids(node="case", name="case-01")
    assert project_ids == ["case-study-01"]


def test_get_node_tsvs(mock_gen3_auth):

    endpoint = "https://test/ok"
    node_name = "demographic"
    master_file_name = "master_demographic.tsv"
    dirpath = "node_tsvs"

    # Clean up Prev Run result if exists
    if os.path.isfile(master_file_name):
        os.remove(master_file_name)

    if os.path.exists(dirpath) and os.path.isdir(dirpath):
        shutil.rmtree(dirpath)

    def _mock_sub_query(query_txt):
        mocked_result = {}
        if query_txt == """{project (first:0){project_id}}""":
            mocked_result = {
                "data": {
                    "project": [
                        {"project_id": "Canine-Osteosarcoma"},
                        {"project_id": "Canine-B_cell_lymphoma"},
                    ]
                }
            }

        return mocked_result

    def _mock_request(url, **kwargs):
        mocked_response = MagicMock(requests.Response)
        if url.endswith(
            "/api/v0/submission/Canine/Osteosarcoma/export/?node_label=demographic&format=tsv"
        ):
            mocked_response.status_code = 200
            mocked_response.text = tsv_header + tsv_result1
        elif url.endswith(
            "/api/v0/submission/Canine/B_cell_lymphoma/export/?node_label=demographic&format=tsv"
        ):
            mocked_response.status_code = 200
            mocked_response.text = tsv_header + tsv_result2
        return mocked_response

    mocked_submission = Gen3Submission(endpoint, mock_gen3_auth)
    mocked_submission.query = _mock_sub_query
    exp = Gen3Expansion(endpoint, mock_gen3_auth)
    exp.sub = mocked_submission

    with patch("gen3.submission.requests.get") as mock_request_get:
        mock_request_get.side_effect = _mock_request
        node_tsvs = exp.get_node_tsvs(node_name)
        # Test both function output and file content
        assert node_tsvs == tsv_header + tsv_result2 + tsv_result1
        assert os.path.isfile("master_demographic.tsv")
        with open(master_file_name, "r") as file:
            data = file.read()
            assert node_tsvs == data

    # Clean up in between tests
    if os.path.isfile(master_file_name):
        os.remove(master_file_name)

    if os.path.exists(dirpath) and os.path.isdir(dirpath):
        shutil.rmtree(dirpath)

    # Test targeted node project request
    with patch("gen3.submission.requests.get") as mock_request_get:
        mock_request_get.side_effect = _mock_request
        node_tsvs = exp.get_node_tsvs(node=node_name, projects="Canine-B_cell_lymphoma")
        assert node_tsvs == tsv_header + tsv_result2
        assert os.path.isfile("master_demographic.tsv")
        with open(master_file_name, "r") as file:
            data = file.read()
            assert node_tsvs == data

    # Final Clean up
    if os.path.isfile(master_file_name):
        os.remove(master_file_name)

    if os.path.exists(dirpath) and os.path.isdir(dirpath):
        shutil.rmtree(dirpath)


def test_get_project_tsvs(mock_gen3_auth):

    endpoint = "https://test/ok"
    node_name = "demographic"
    dirpath = "project_tsvs"
    filename1 = "Canine-B_cell_lymphoma_demographic.tsv"
    filename2 = "Canine-B_cell_lymphoma_diagnosis.tsv"

    # Clean up Prev Run result if exists
    if os.path.exists(dirpath) and os.path.isdir(dirpath):
        shutil.rmtree(dirpath)

    def _mock_sub_query(query_txt):
        mocked_result = {}
        if query_txt == """{project (first:0){project_id}}""":
            mocked_result = {
                "data": {"project": [{"project_id": "Canine-B_cell_lymphoma"}]}
            }
        elif query_txt == """{_node_type (first:-1) {id}}""":
            mocked_result = {
                "data": {
                    "_node_type": [
                        {"id": "demographic"},
                        {"id": "diagnosis"},
                        {"id": "program"},
                    ]
                }
            }
        elif (
            query_txt
            == """{_demographic_count (project_id:"Canine-B_cell_lymphoma")}"""
        ):
            mocked_result = {"data": {"_demographic_count": 1}}
        elif (
            query_txt == """{_diagnosis_count (project_id:"Canine-B_cell_lymphoma")}"""
        ):
            mocked_result = {"data": {"_diagnosis_count": 1}}

        return mocked_result

    def _mock_request(url, **kwargs):
        mocked_response = MagicMock(requests.Response)
        if url.endswith(
            "/api/v0/submission/Canine/B_cell_lymphoma/export/?node_label=demographic&format=tsv"
        ):
            mocked_response.status_code = 200
            mocked_response.text = tsv_header + tsv_result2
        elif url.endswith(
            "/api/v0/submission/Canine/B_cell_lymphoma/export/?node_label=diagnosis&format=tsv"
        ):
            mocked_response.status_code = 200
            mocked_response.text = tsv_header + tsv_result3
        return mocked_response

    mocked_submission = Gen3Submission(endpoint, mock_gen3_auth)
    mocked_submission.query = _mock_sub_query
    exp = Gen3Expansion(endpoint, mock_gen3_auth)
    exp.sub = mocked_submission

    with patch("gen3.submission.requests.get") as mock_request_get:
        mock_request_get.side_effect = _mock_request
        project_tsvs = exp.get_project_tsvs()
        # Test function output (downloaded file names)
        project_tsv_filenames = (
            filename1 + "\n" + filename2 + "\n"
        )  #'Canine-B_cell_lymphoma_demographic.tsv\nCanine-B_cell_lymphoma_diagnosis.tsv\n'
        assert project_tsvs == project_tsv_filenames
        # Test Downloaded files exists and expected content
        filepath = dirpath + "/Canine-B_cell_lymphoma_tsvs/"
        filenames = [filepath + filename1, filepath + filename2]
        for filename in filenames:
            assert os.path.isfile(filename)
            with open(filename, "r") as file:
                data = file.read()
                if filename == filename1:
                    assert data == tsv_header + tsv_result2
                elif filename == filename2:
                    assert data == tsv_header + tsv_result3

    # Clean up in between tests
    if os.path.exists(dirpath) and os.path.isdir(dirpath):
        shutil.rmtree(dirpath)

    # Test targeted node project request
    with patch("gen3.submission.requests.get") as mock_request_get:
        mock_request_get.side_effect = _mock_request
        project_tsvs = exp.get_project_tsvs(
            nodes="demographic", projects="Canine-B_cell_lymphoma"
        )
        # Test function output (downloaded file names)
        project_tsv_filenames = filename1 + "\n"
        assert project_tsvs == project_tsv_filenames
        # Test Downloaded files exists and expected content
        filepath = dirpath + "/Canine-B_cell_lymphoma_tsvs/"
        filename = filepath + filename1

        assert os.path.isfile(filename)
        with open(filename, "r") as file:
            data = file.read()
            assert data == tsv_header + tsv_result2

    # Clean up
    if os.path.exists(dirpath) and os.path.isdir(dirpath):
        shutil.rmtree(dirpath)
