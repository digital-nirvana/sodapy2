import json

from requests import Response


class SodaResponse(Response):
    """Return a copy of the requests.Response object."""

    def __init__(self, response):
        super().__init__()
        for k, v in list(response.__dict__.items()):
            setattr(self, k, v)

    def __repr__(self):
        return f"<SodaResponse [{self.status_code}]>"

    def raise_for_status(self) -> None:
        """Override raise_for_status() to include the Socrata API error information."""

        if 400 <= self.status_code < 600:
            raise SodaHTTPError(
                status_code=self.status_code,
                reason=self.reason,
                request_url=self.url,
                error_detail=self.text,
            )


class SodaHTTPError(Exception):
    """Raise a specialized HTTP Error that includes extra error information."""

    def __init__(self, status_code, reason, *args, **kwargs):
        """
        Socrata returns detailed error information that can look like this:
        {
            "code": "query.compiler.malformed",
            "error": true,
            "message": "Could not parse SoQL query \"select * where string_column > 42\"",
            "data": {
                "query": "select * where string_column > 42"
            }
        }

        But not all of these fields may be present in all cases.
        """
        super().__init__(*args)

        self.status_code = status_code
        self.reason = reason
        self.request_url = kwargs.pop("request_url", "")

        self.error_detail = kwargs.pop("error_detail", {})
        if isinstance(self.error_detail, str):
            try:
                self.error_detail = json.loads(self.error_detail)
            except json.JSONDecodeError:
                pass  # Pass because self.error_detail is already set to {}.

    def __str__(self):
        if 400 <= self.status_code < 500:
            error_msg = f"Client Error {self.status_code}: {self.reason}. See error_detail."
            if self.request_url:
                error_msg = (
                    f"Client Error {self.status_code}: {self.reason} for url {self.request_url}. See error_detail."
                )
        elif 500 <= self.status_code < 600:
            error_msg = f"Server Error {self.status_code}: {self.reason}. See error_detail."
            if self.request_url:
                error_msg = (
                    f"Server Error {self.status_code}: {self.reason} for url {self.request_url}. See error_detail."
                )
        else:
            error_msg = f"Undefined Error {self.status_code}: {self.reason}"
        return error_msg

    @property
    def code(self) -> str:
        return self.error_detail.get("code", "")

    @property
    def error(self) -> bool:
        return self.error_detail.get("error", True)

    @property
    def message(self) -> str:
        return self.error_detail.get("message", "")

    @property
    def query(self) -> dict:
        data = self.error_detail.get("data", {})
        return data.get("query", "")
