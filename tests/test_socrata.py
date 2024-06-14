import inspect
import json
import logging
import os.path
from urllib.parse import urlunsplit

import pytest
import requests_mock

from sodapy2 import Socrata
from sodapy2.constants import Formats, SodaApiEndpoints

APPTOKEN = "FakeAppToken"
DATASET_ID = "sodapy2-pytest"
DOMAIN = "fakedomain.com"
LOGGER = logging.getLogger(__name__)
PROTO = "http+mock"


def test_client():
    client = Socrata(DOMAIN, APPTOKEN)
    assert isinstance(client, Socrata)
    client.close()


def test_client_throttle_warning(caplog):
    with caplog.at_level(logging.WARNING):
        client = Socrata(DOMAIN, None)
    assert "strict throttling limits" in caplog.text
    client.close()


def test_context_manager():
    with Socrata(DOMAIN, APPTOKEN) as client:
        assert isinstance(client, Socrata)


def test_context_manager_no_domain_exception():
    with pytest.raises(Exception):
        with Socrata(None, APPTOKEN):
            pass


def test_context_manager_timeout_exception():
    with pytest.raises(TypeError):
        with Socrata(DOMAIN, APPTOKEN, timeout="fail"):
            pass


def test_get_invalid_type():
    with pytest.raises(ValueError) as exc_info:
        with Socrata(DOMAIN, APPTOKEN) as client:
            client.get(dataset_id=DATASET_ID, content_type="error")
    assert str(exc_info.value) == "content_type must be one of: ['csv', 'json', 'rdfxml', 'xml']"


def test_get_json():
    response_body = _get_test_response_body(body_file="get_songs.json")
    adapter = requests_mock.Adapter()
    adapter.register_uri(
        "GET",
        urlunsplit((PROTO, DOMAIN, f"{SodaApiEndpoints.DATASET.endpoint}/{DATASET_ID}", None, None)),
        json=response_body,
        headers={"content-type": Formats.JSON.mimetype},
    )

    with Socrata(DOMAIN, APPTOKEN, session_adapter=adapter) as client:
        response = client.get(DATASET_ID)
        assert isinstance(response, list)
        assert len(response) == 10


def test_get_json_with_unicode():
    response_body = _get_test_response_body(body_file="get_songs_unicode.json")
    adapter = requests_mock.Adapter()
    adapter.register_uri(
        "GET",
        urlunsplit((PROTO, DOMAIN, f"{SodaApiEndpoints.DATASET.endpoint}/{DATASET_ID}", None, None)),
        json=response_body,
        headers={"content-type": Formats.JSON.mimetype},
    )

    with Socrata(DOMAIN, APPTOKEN, session_adapter=adapter) as client:
        response = client.get(DATASET_ID)
        assert isinstance(response, list)
        assert len(response) == 10


def test_get_all():
    adapter = requests_mock.Adapter()

    response_body = _get_test_response_body(body_file="bike_counts_page_1.json")
    adapter.register_uri(
        "GET",
        urlunsplit((PROTO, DOMAIN, f"{SodaApiEndpoints.DATASET.endpoint}/{DATASET_ID}", "$offset=0", None)),
        json=response_body,
        headers={"content-type": Formats.JSON.mimetype},
    )

    response_body = _get_test_response_body(body_file="bike_counts_page_2.json")
    adapter.register_uri(
        "GET",
        urlunsplit((PROTO, DOMAIN, f"{SodaApiEndpoints.DATASET.endpoint}/{DATASET_ID}", "$offset=1000", None)),
        json=response_body,
        headers={"content-type": Formats.JSON.mimetype},
    )

    with Socrata(DOMAIN, APPTOKEN, session_adapter=adapter) as client:
        response = client.get_all(dataset_id=DATASET_ID)
        assert inspect.isgenerator(response)
        data = list(response)
        assert len(data) == 1001
        assert data[0]["date"] == "2016-09-21T15:45:00.000"
        assert data[-1]["date"] == "2016-10-02T01:45:00.000"


def test_get_datasets():
    response_body = _get_test_response_body(body_file="get_datasets.json")
    adapter = requests_mock.Adapter()
    adapter.register_uri(
        "GET",
        urlunsplit((PROTO, DOMAIN, SodaApiEndpoints.DISCOVERY.endpoint, "limit=7&offset=0", None)),
        json=response_body,
        headers={"content-type": Formats.JSON.mimetype},
    )
    print(adapter)

    with Socrata(DOMAIN, APPTOKEN, session_adapter=adapter) as client:
        response = client.get_datasets(limit=7)
        assert isinstance(response, list)
        assert len(response) == 7


# TODO: Implement this.
# def test_get_all_metadata():
#     response_body = _get_test_response_body(body_file="get_all_metadata")
#     adapter = requests_mock.Adapter()
#     adapter.register_uri(
#         "GET",
#         urlunsplit((PROTO, DOMAIN, f"{SODAAPI_METADATA_ENDPT}", None, None)),
#         json=response_body,
#         headers={"content-type": Formats.JSON.mimetype},
#     )

#     with Socrata(DOMAIN, APPTOKEN, session_adapter=adapter) as client:
#         response = client.get_metadata()
#         assert isinstance(response, dict)
#         assert "newBackend" in response
#         assert "attachments" in response["metadata"]


def test_get_dataset_metadata():
    response_body = _get_test_response_body(body_file="get_dataset_metadata.json")
    adapter = requests_mock.Adapter()
    adapter.register_uri(
        "GET",
        urlunsplit((PROTO, DOMAIN, f"{SodaApiEndpoints.METADATA.endpoint}/{DATASET_ID}", None, None)),
        json=response_body,
        headers={"content-type": Formats.JSON.mimetype},
    )

    with Socrata(DOMAIN, APPTOKEN, session_adapter=adapter) as client:
        response = client.get_metadata(DATASET_ID)
        assert isinstance(response, dict)
        assert "id" in response
        assert response["id"] == DATASET_ID
        assert "name" in response


def _get_test_response_body(body_file: str):
    path = os.path.join(
        os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))),  # type: ignore
        "test_data",
        body_file,
    )
    with open(path, "r") as f:
        if body_file.endswith(".json"):
            body = json.load(f)
        else:
            body = f.read()
    return body
