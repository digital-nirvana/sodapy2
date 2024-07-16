import pytest

from sodapy2 import Socrata


@pytest.fixture
def client():
    domain = "data.seattle.gov"  # Use the same domain that Socrata uses in their examples.
    # app_token = "example_app_token"
    return Socrata(domain=domain)


@pytest.fixture
def dataset_id():
    return "jguv-t9rb"


def test_get_json(client, dataset_id):
    response = client.get(dataset_id, content_type="json")
    assert isinstance(response, list)
    assert len(response) == 1000  # 1000 is the default limit.
    for item in response:
        assert isinstance(item, dict)


def test_get_json_with_params(client, dataset_id):
    response = client.get(dataset_id, content_type="json", select="species,license_number")
    assert isinstance(response, list), "Response object should be a list"
    assert len(response) > 0, "Response object shouldn't be empty"
    for item in response:
        assert isinstance(item, dict), "Response item should be dict"
        assert {"species", "license_number"}.issubset(
            item.keys()
        ), "Response item should include fields (species, license_number)"


def test_get_json_paging(client, dataset_id):
    complete_response = []
    for i in range(0, 30, 10):
        response = client.get(dataset_id, content_type="json", limit=10, offset=i)
        assert isinstance(response, list), "Response object should be a list"
        assert len(response) == 10, "Response should be size 10"
        complete_response.extend(response)

    assert len(complete_response) == 30, "Complete response should be size 30"
    for item in response:
        assert isinstance(item, dict), "Response item should be dict"


def test_get_csv(client, dataset_id):
    response = client.get(dataset_id, content_type="csv", limit=5)
    assert isinstance(response, list)
    assert len(response) == 6, "Expecting 6 rows; 5 data + 1 header"
    for item in response:
        assert isinstance(item, list)


def test_get_datasets(client):
    response = client.get_datasets(limit=2)
    assert isinstance(response, dict)
    assert {"results", "resultSetSize", "timings"}.issubset(response.keys())
    assert isinstance(response["resultSetSize"], int)
    assert isinstance(response["timings"], dict)
    assert isinstance(response["results"], list)
    assert len(response["results"]) == 2

    for result in response["results"]:
        assert {"name", "id", "createdAt"}.issubset(result["resource"].keys())


def test_get_dataset_metadata(client, dataset_id):
    response = client.get_metadata(dataset_id=dataset_id)
    assert isinstance(response, dict)
    assert {"name", "id", "createdAt", "dataUri"}.issubset(response.keys())
    assert response["id"] == dataset_id


def test_get_all_metadata(client):
    client.timeout = 30  # This request can take longer than the default timeout.
    response = client.get_metadata()
    assert isinstance(response, list)
    for r in response:
        print(r)
        assert {"name", "id", "createdAt", "dataUri"}.issubset(r.keys())
