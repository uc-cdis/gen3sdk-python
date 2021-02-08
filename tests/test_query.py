import json
import requests
from unittest.mock import MagicMock, patch


@patch("gen3.jobs.requests.post")
def test_query(requests_post_mock, gen3_query):
    data_type = "subject"
    records = [
        {"id": "uuid1", "vital_status": "Alive"},
        {"id": "uuid2", "vital_status": "Alive"},
        {"id": "uuid3", "vital_status": "Alive"},
        {"id": "uuid4", "vital_status": "Alive"},
    ]
    filters = {"vital_status": "Alive"}
    filter_object = {"AND": [{"=": {"vital_status": "Alive"}}]}

    def _mock_request(url, **kwargs):
        mocked_response = MagicMock(requests.Response, status_code=200)
        if url.endswith("/guppy/graphql"):
            assert kwargs["json"]["variables"]["filter"] == filter_object
            mocked_response.json.return_value = {"data": {data_type: records}}
        elif url.endswith("/guppy/download"):
            assert kwargs["json"]["filter"] == filter_object
            mocked_response.json.return_value = records
        return mocked_response

    requests_post_mock.side_effect = _mock_request

    # hit "/guppy/graphql" endpoint. Use "filters" param, which should
    # be converted to a filter object
    data = gen3_query.query(
        data_type=data_type,
        fields=["id", "vital_status"],
        first=4,
        offset=2,
        filters=filters,
        sort_object={"id": "asc"},
    )
    # "first" and "offset" are handled on the _server_ side
    assert data == {"data": {data_type: records}}

    # hit "/guppy/graphql" endpoint. Use "filter_object" param
    data = gen3_query.query(
        data_type=data_type,
        fields=["id", "vital_status"],
        first=4,
        offset=2,
        filter_object=filter_object,
        sort_object={"id": "asc"},
    )
    # "first" and "offset" are handled on the _server_ side
    assert data == {"data": {data_type: records}}

    # first + offset > 10,000 triggers use of the "/guppy/download"
    # endpoint instead of the "/guppy/graphql" endpoint
    data = gen3_query.query(
        data_type=data_type,
        fields=["id", "vital_status"],
        first=9999,
        offset=2,
        filters=filters,
        sort_object={"id": "asc"},
    )
    # "first" and "offset" are handled on the _client_ side
    assert data == {"data": {data_type: records[2:]}}
