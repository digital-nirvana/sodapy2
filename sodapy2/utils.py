import os

import requests


def prune_empty_values(dictionary: dict) -> dict:
    """Remove null elements from a dict."""
    pruned = {}
    for k, v in dictionary.items():
        if v is not None:  # Specifically looking for None, not just falsey values.
            pruned[k] = v
    return pruned


def download_file(url: str, local_fpath: str) -> None:
    """
    Utility function that downloads a chunked response from the specified url to a local path.
    This method is suitable for larger downloads.
    """
    response = requests.get(url, stream=True)
    response.raise_for_status()
    os.makedirs(os.path.dirname(local_fpath), exist_ok=True)

    with open(local_fpath, "wb") as outfile:
        for chunk in response.iter_content(chunk_size=1024):
            if chunk:  # filter out keep-alive new chunks.
                outfile.write(chunk)


def raise_for_status(response: requests.Response) -> None:
    """
    Custom raise_for_status with more appropriate error message.

    Args:
        response: a response object.
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
