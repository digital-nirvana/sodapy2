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
