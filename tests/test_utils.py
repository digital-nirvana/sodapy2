import pytest
import requests
import requests_mock

import sodapy2.utils as utils


@pytest.mark.parametrize(
    ("status_code", "status_type", "reason", "raises_exception"),
    [
        (200, "Success", "OK", False),
        (300, "Redirection", "Multiple Choices", False),
        (400, "Client Error", "Bad Request", True),
        (500, "Server Error", "Internal Server Error", True),
        (600, "Foo Bar", "Here be dragons", False),
    ],
)
def test_raise_for_status(status_code, status_type, reason, raises_exception):
    response = requests.models.Response()
    response.status_code = status_code
    response.reason = reason

    if raises_exception:
        with pytest.raises(
            requests.exceptions.HTTPError,
            match=f"{status_code} {status_type}: {reason}",
        ):
            utils.raise_for_status(response)
    else:
        utils.raise_for_status(response)


@pytest.mark.parametrize(
    ("elems", "result"),
    [
        ({}, {}),
        ({"a": 1, "b": None, "c": "d"}, {"a": 1, "c": "d"}),
        ({"s": "", "c": 0}, {"s": "", "c": 0}),
    ],
)
def test_prune_empty_values(elems, result):
    assert utils.prune_empty_values(elems) == result


def test_download_file(tmp_path):
    path = tmp_path / "myfile.txt"
    url = "http://fileserver.dev/file"
    text = "the response data"
    with requests_mock.Mocker() as mock:
        mock.get(url, text=text)
        utils.download_file(url, str(path))
    assert path.read_text() == text
