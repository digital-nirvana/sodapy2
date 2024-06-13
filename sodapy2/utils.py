import requests

from .constants import DEFAULT_API_PATH, OLD_API_PATH


def raise_for_status(response: requests.Response) -> None:
    """
    Custom raise_for_status with more appropriate error message.
    """
    http_error_msg = ""

    if 400 <= response.status_code < 500:
        http_error_msg = f"{response.status_code} Client Error: {response.reason}"

    elif 500 <= response.status_code < 600:
        http_error_msg = f"{response.status_code} Server Error: {response.reason}"

    if http_error_msg:
        try:
            more_info = response.json().get("message")
        except ValueError:  # This will raise if there is no JSON response.
            more_info = None
        if more_info and more_info.lower() != response.reason.lower():
            http_error_msg += f".\n\t{more_info}"
        raise requests.exceptions.HTTPError(http_error_msg, response=response)


def clear_empty_values(args) -> dict:
    """
    Scrap junk data from a dict.
    """
    result = {}
    for param in args:
        if args[param] is not None:
            result[param] = args[param]
    return result


def format_old_api_request(dataset_id: str = "", content_type: str = "") -> str:
    if dataset_id:
        if content_type:
            return f"{OLD_API_PATH}/{dataset_id}.{content_type}"
        return f"{OLD_API_PATH}/{dataset_id}"
    if content_type:
        return f"{OLD_API_PATH}.{content_type}"
    raise Exception("This method requires at least a dataset_id or content_type.")


def format_new_api_request(dataset_id: str, content_type: str, row_id: str = "") -> str:
    if row_id:
        return f"{DEFAULT_API_PATH}{dataset_id}/{row_id}.{content_type}"
    return f"{DEFAULT_API_PATH}{dataset_id}.{content_type}"


def authentication_validation(username: str, password: str, access_token: str) -> None:
    """
    Only accept one form of authentication.
    """
    if bool(username) is not bool(password):
        raise Exception("Basic authentication requires a username AND password.")
    if (username and access_token) or (password and access_token):
        raise Exception("Must use only one authn method: Basic or OAuth2.0.")


def download_file(url: str, local_filename: str) -> None:
    """
    Utility function that downloads a chunked response from the specified url to a local path.
    This method is suitable for larger downloads.
    """
    response = requests.get(url, stream=True)
    with open(local_filename, "wb") as outfile:
        for chunk in response.iter_content(chunk_size=1024):
            if chunk:  # filter out keep-alive new chunks.
                outfile.write(chunk)
