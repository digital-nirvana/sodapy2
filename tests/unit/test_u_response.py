from unittest.mock import MagicMock

import pytest

from sodapy2.response import SodaHTTPError, SodaResponse


class TestSodaResponse:
    request_url = "https://example.com/"

    @pytest.fixture
    def mock_200_response(self):
        """Fixture to create a mock Response object."""
        response = MagicMock(spec=SodaResponse)
        response.status_code = 200
        response.reason = "OK"
        response.url = self.request_url
        return response

    @pytest.fixture
    def mock_404_response(self):
        """Fixture to create a mock Response object."""
        response = MagicMock(spec=SodaResponse)
        response.status_code = 404
        response.reason = "Not Found"
        response.url = self.request_url
        return response

    @pytest.fixture
    def mock_500_response(self):
        """Fixture to create a mock Response object."""
        response = MagicMock(spec=SodaResponse)
        response.status_code = 500
        response.reason = "Internal Server Error"
        response.url = self.request_url
        return response

    def test_initialization(self, mock_200_response: MagicMock):
        """Test that SodaResponse initializes correctly."""
        soda_response = SodaResponse(mock_200_response)
        assert soda_response.status_code == 200
        assert soda_response.reason == "OK"
        assert soda_response.url == self.request_url

    def test_repr(self, mock_200_response: MagicMock):
        """Test the __repr__ method."""
        soda_response = SodaResponse(mock_200_response)
        assert repr(soda_response) == "<SodaResponse [200]>"

    def test_raise_for_status_no_error(self, mock_200_response: MagicMock):
        """Test that raise_for_status() does not raise an error for 2xx status codes."""
        soda_response = SodaResponse(mock_200_response)
        try:
            soda_response.raise_for_status()
        except Exception as e:
            pytest.fail(f"raise_for_status() raised {type(e).__name__} unexpectedly!")

    def test_raise_for_status_4xx(self, mock_404_response: MagicMock):
        """Test that raise_for_status() raises an error for 4xx status codes."""
        soda_response = SodaResponse(mock_404_response)

        with pytest.raises(SodaHTTPError, match=r"Client Error 4\d\d: .+") as context:
            soda_response.raise_for_status()

        assert context.value.status_code == 404
        assert context.value.reason == "Not Found"
        assert context.value.request_url == self.request_url

    def test_raise_for_status_5xx(self, mock_500_response: MagicMock):
        """Test that raise_for_status() raises an error for 4xx status codes."""
        soda_response = SodaResponse(mock_500_response)

        with pytest.raises(SodaHTTPError, match=r"Server Error 5\d\d: .+") as context:
            soda_response.raise_for_status()

        assert context.value.status_code == 500
        assert context.value.reason == "Internal Server Error"
        assert context.value.request_url == self.request_url


class TestSodaHTTPError:
    request_url = "https://example.com/"

    def test_server_error(self):
        test_status_code = 502
        test_reason = "Bad Gateway"

        with pytest.raises(SodaHTTPError) as exc:
            raise SodaHTTPError(status_code=test_status_code, reason=test_reason)
        assert exc.value.status_code == test_status_code
        assert exc.value.reason == test_reason
        assert str(exc.value) == f"Server Error {test_status_code}: {test_reason}. See error_detail."

    def test_server_error_with_request_url(self):
        test_status_code = 502
        test_reason = "Bad Gateway"

        with pytest.raises(SodaHTTPError) as exc:
            raise SodaHTTPError(status_code=test_status_code, reason=test_reason, request_url=self.request_url)
        assert exc.value.status_code == test_status_code
        assert exc.value.reason == test_reason
        assert exc.value.request_url == self.request_url
        assert (
            str(exc.value)
            == f"Server Error {test_status_code}: {test_reason} for url {self.request_url}. See error_detail."
        )

    def test_client_error(self):
        test_status_code = 404
        test_reason = "Not Found"

        with pytest.raises(SodaHTTPError) as exc:
            raise SodaHTTPError(status_code=test_status_code, reason=test_reason)
        assert exc.value.status_code == test_status_code
        assert exc.value.reason == test_reason
        assert str(exc.value) == f"Client Error {test_status_code}: {test_reason}. See error_detail."

    def test_client_error_with_request_url(self):
        test_status_code = 404
        test_reason = "Not Found"

        with pytest.raises(SodaHTTPError) as exc:
            raise SodaHTTPError(status_code=test_status_code, reason=test_reason, request_url=self.request_url)
        assert exc.value.status_code == test_status_code
        assert exc.value.reason == test_reason
        assert exc.value.request_url == self.request_url
        assert (
            str(exc.value)
            == f"Client Error {test_status_code}: {test_reason} for url {self.request_url}. See error_detail."
        )

    def test_nonerror_code(self):
        test_status_code = 300
        test_reason = "Multiple Choices"

        with pytest.raises(SodaHTTPError) as exc:
            raise SodaHTTPError(status_code=test_status_code, reason=test_reason)
        assert exc.value.status_code == test_status_code
        assert exc.value.reason == test_reason
        assert str(exc.value) == f"Undefined Error {test_status_code}: {test_reason}"

    def test_error_info(self):
        test_status_code = 400
        test_reason = "Bad Request"
        test_error_detail = {
            "code": "error code",
            "error": "error",
            "message": 'Could not parse SoQL query "select * where string_column > 42"',
            "data": {"query": "select * where string_column > 42"},
        }

        with pytest.raises(SodaHTTPError) as exc:
            raise SodaHTTPError(status_code=test_status_code, reason=test_reason, error_detail=test_error_detail)
        assert exc.value.status_code == test_status_code
        assert exc.value.reason == test_reason
        assert str(exc.value) == f"Client Error {test_status_code}: {test_reason}. See error_detail."
        assert exc.value.code == test_error_detail["code"]
        assert exc.value.error == test_error_detail["error"]
        assert exc.value.message == test_error_detail["message"]
        assert exc.value.query == test_error_detail["data"]["query"]
