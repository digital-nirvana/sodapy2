import pytest
import requests_mock

import sodapy2.utils as utils


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
