import csv
import json
import logging
import os
import re
from io import StringIO
from typing import Any, Generator, Union
from urllib.parse import urlunsplit

import requests

import sodapy2.utils as utils
from sodapy2.constants import DATASETS_PATH


class Socrata:
    """
    The main class that interacts with the SODA API. Sample usage:
        from sodapy2 import Socrata
        client = Socrata("opendata.socrata.com", None)
    """

    DEFAULT_LIMIT = 1000  # See https://dev.socrata.com/docs/paging.html#2.1
    PROTO = "https"

    def __init__(
        self,
        domain,
        app_token,
        username=None,
        password=None,
        access_token=None,
        session_adapter=None,
        timeout=10,
    ):
        """
        The required arguments are:
            domain: the domain you wish you to access
            app_token: your Socrata application token
        Simple requests are possible without an app_token, though these
        requests will be rate-limited.

        For write/update/delete operations or private datasets, the Socrata API
        currently supports basic HTTP authentication, which requires these
        additional parameters.
            username: your Socrata username
            password: your Socrata password

        The basic HTTP authentication comes with a deprecation warning, and the
        current recommended authentication method is OAuth 2.0. To make
        requests on behalf of the user using OAuth 2.0 authentication, follow
        the recommended procedure and provide the final access_token to the
        client.

        More information about authentication can be found in the official
        docs:
            http://dev.socrata.com/docs/authentication.html
        """
        if not domain:
            raise Exception("A domain is required.")
        self.domain = domain

        # set up the session with proper authentication crendentials
        self.session = requests.Session()
        if not app_token:
            logging.warning("Requests made without an app_token will be" " subject to strict throttling limits.")
        else:
            self.session.headers.update({"X-App-token": app_token})

        utils.authentication_validation(username, password, access_token)

        # use either basic HTTP auth or OAuth2.0
        if username and password:
            self.session.auth = (username, password)
        elif access_token:
            self.session.headers.update({"Authorization": f"OAuth {access_token}"})

        if session_adapter:
            self.session.mount(prefix=f"{self.PROTO}://", adapter=session_adapter["adapter"])

        if not isinstance(timeout, (int, float)):
            raise TypeError("Timeout must be numeric.")
        self.timeout = timeout

    def __enter__(self):
        """
        This runs as the with block is set up.
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        This runs at the end of a with block. It simply closes the client.
        """
        self.close()

    def datasets(self, limit: int = 0, offset: int = 0, order: str = "", **kwargs):
        """
        Returns the list of datasets associated with a particular domain.
        This method performs a GET request on these type of URLs: e.g. https://data.edmonton.ca/api/catalog/v1


        WARNING: Large limits (>1000) will return many megabytes of data which can be slow
        on low-bandwidth networks, and is also a lot of data to hold in memory.

        Args:
            limit: max number of results to return, default is all (0)
            offset: the offset of result set
            order: field to sort on, optionally with ' ASC' or ' DESC' suffix
            kwargs:
                ids: list of dataset IDs to consider
                domains: list of additional domains to search
                categories: list of categories
                tags: list of tags
                only: list of logical types to return, among `api`, `calendar`,
                    `chart`, `datalens`, `dataset`, `federated_href`, `file`,
                    `filter`, `form`, `href`, `link`, `map`, `measure`, `story`,
                    `visualization`
                shared_to: list of users IDs or team IDs that datasets have to be
                    shared with, or the string `site` meaning anyone on the domain.
                    Note that you may only specify yourself or a team that you are
                    on.
                    Also note that if you search for assets shared to you, assets
                    owned by you might be not be returned.
                column_names: list of column names that must be present in the
                    tabular datasets
                q: text query that will be used by Elasticsearch to match results
                min_should_match: string specifying the number of words from `q`
                    that should match. Refer to Elasticsearch docs for the format,
                    the default is '3<60%', meaning that 60% of the terms must
                    match, or all of them if there are 3 or fewer.
                attribution: string specifying the organization datasets must come
                    from
                license: string used to filter on results having a specific license
                derived_from: string containing the ID of a dataset that must be a
                    parent of the result datasets (for example, charts are derived
                    from a parent dataset)
                provenance: string 'official' or 'community'
                for_user: string containing a user ID that must own the returned
                    datasets
                visibility: string 'open' or 'internal'
                public: boolean indicating that all returned datasets should be
                    public (True) or private (False)
                published: boolean indicating that returned datasets should have
                    been published (True) or not yet published (False)
                approval_status: string 'pending', 'rejected', 'approved',
                    'not_ready' filtering results by their current status in the
                    approval pipeline
                explicitly_hidden: boolean filtering out datasets that have been
                    explicitly hidden on a domain (False) or returning only those
                    (True)
                derived: boolean allowing to search only for derived datasets
                    (True) or only those from which other datasets were derived
                    (False)
        """
        # Those filters can be passed multiple times; this function expects an iterable for them.
        filter_multiple = set(
            [
                "ids",
                "domains",
                "categories",
                "tags",
                "only",
                "shared_to",
                "column_names",
            ]
        )
        # Those filters only get a single value.
        filter_single = set(
            [
                "q",
                "min_should_match",
                "attribution",
                "license",
                "derived_from",
                "provenance",
                "for_user",
                "visibility",
                "public",
                "published",
                "approval_status",
                "explicitly_hidden",
                "derived",
            ]
        )
        all_filters = filter_multiple.union(filter_single)
        for key in kwargs:
            if key not in all_filters:
                raise TypeError("Unexpected keyword argument %s" % key)
        params = [("domains", self.domain)]
        if limit:
            params.append(("limit", limit))
        for key, value in kwargs.items():
            if key in filter_multiple:
                for item in value:
                    params.append((key, item))
            elif key in filter_single:
                params.append((key, value))
        # TODO: custom domain-specific metadata
        # https://socratadiscovery.docs.apiary.io/reference/0/find-by-domain-specific-metadata

        if order:
            params.append(("order", order))

        results = self._perform_request("get", DATASETS_PATH, params=params + [("offset", offset)])
        num_results = results["resultSetSize"]
        # no more results to fetch, or limit reached
        if limit >= num_results or limit == len(results["results"]) or num_results == len(results["results"]):
            return results["results"]

        if limit != 0:
            raise Exception(
                f"Unexpected number of results returned from endpoint. Expected {limit}, got {len(results["results"])}."
            )

        # get all remaining results
        all_results = results["results"]
        while len(all_results) != num_results:
            offset += len(results["results"])
            results = self._perform_request("get", DATASETS_PATH, params=params + [("offset", offset)])
            all_results.extend(results["results"])

        return all_results

    def download_attachments(
        self,
        dataset_id: str,
        content_type: str = "json",
        download_dir: str = "~/sodapy_downloads",
    ) -> list:
        """
        Download all of the attachments associated with a dataset.

        Args:
            dataset_id: The identifier of the desired dataset.
            content_type: Options are json, csv, and xml.
            download_dir: A local file path where content will be stored.

        Returns:
            The paths of downloaded files.
        """
        metadata = self.get_metadata(dataset_id, content_type=content_type)
        files: list = []
        attachments = metadata["metadata"].get("attachments")
        if not attachments:
            logging.info("No attachments were found or downloaded.")
            return files

        download_dir = os.path.join(os.path.expanduser(download_dir), dataset_id)
        os.makedirs(download_dir, exist_ok=True)

        for attachment in attachments:
            file_path = os.path.join(download_dir, attachment["filename"])
            has_assetid = attachment.get("assetId", False)
            if has_assetid:
                base = utils.format_old_api_request(dataset_id=dataset_id)
                assetid = attachment["assetId"]
                resource = f"{base}/files/{assetid}?download=true&filename={attachment["filename"]}"
            else:
                base = "/api/assets"
                assetid = attachment["blobId"]
                resource = f"{base}/{assetid}?download=true"

            uri = urlunsplit((self.PROTO, self.domain, resource, None, None))
            utils.download_file(uri, file_path)
            files.append(file_path)

        logging.info("The following files were downloaded:\n\t%s", "\n\t".join(files))
        return files

    def get(self, dataset_id: str, content_type: str = "json", **kwargs) -> Union[list[list[str]], str]:
        """
        Read data from the requested resource.

        Args:
            dataset_id: The identifier of the desired dataset.
            content_type: Options are json, csv, and xml.
            Optionally, specify a keyword arg to filter results:
                select : the set of columns to be returned, defaults to *
                where : filters the rows to be returned, defaults to limit
                order : specifies the order of results
                group : column to group results on
                limit : max number of results to return, defaults to 1000
                offset : offset, used for paging. Defaults to 0
                q : performs a full text search for a value
                query : full SoQL query string, all as one parameter
                exclude_system_fields : defaults to true. If set to false, the
                    response will include system fields (:id, :created_at, and
                    :updated_at)

        More information about the SoQL parameters can be found at the official
        docs:
            http://dev.socrata.com/docs/queries.html

        More information about system fields can be found here:
            http://dev.socrata.com/docs/system-fields.html
        """
        resource = utils.format_new_api_request(dataset_id=dataset_id, content_type=content_type)
        headers = utils.clear_empty_values({"Accept": kwargs.pop("format", None)})

        # SoQL parameters
        params = {
            "$select": kwargs.pop("select", None),
            "$where": kwargs.pop("where", None),
            "$order": kwargs.pop("order", None),
            "$group": kwargs.pop("group", None),
            "$limit": kwargs.pop("limit", None),
            "$offset": kwargs.pop("offset", None),
            "$q": kwargs.pop("q", None),
            "$query": kwargs.pop("query", None),
            "$$exclude_system_fields": kwargs.pop("exclude_system_fields", None),
        }

        # Additional parameters, such as field names
        params.update(kwargs)
        params = utils.clear_empty_values(params)

        response = self._perform_request("get", resource, headers=headers, params=params)
        return response

    def get_all(self, *args, **kwargs) -> Generator[Any, Any, Any]:
        """
        Read data from the requested resource, paginating over all results.

        Args:
            See optional args in `get()`.

        Returns:
            Generator.
        """
        params = {}
        params.update(kwargs)
        if "offset" not in params:
            params["offset"] = 0
        limit = params.get("limit", self.DEFAULT_LIMIT)

        while True:
            response = self.get(*args, **params)
            for item in response:
                yield item

            if len(response) < limit:
                return
            params["offset"] += limit

    def get_metadata(self, dataset_id: str, content_type: str = "json"):
        """
        Retrieve the metadata for a particular dataset.
        """
        resource = utils.format_old_api_request(dataset_id=dataset_id, content_type=content_type)
        return self._perform_request("get", resource)

    def _perform_request(self, request_type, resource, **kwargs):
        """
        Utility method that performs all requests.
        """
        supported_http_methods = frozenset(["get"])
        if request_type not in supported_http_methods:
            raise Exception(f"Unknown HTTP request method. Supported methods are {supported_http_methods}")

        uri = urlunsplit((self.PROTO, self.domain, resource, None, None))
        kwargs["timeout"] = self.timeout
        response = getattr(self.session, request_type)(uri, **kwargs)

        if response.status_code not in (200, 202):
            utils.raise_for_status(response)

        # When responses have no content body (ie. delete, set_permission), simply return the whole response.
        if not response.text:
            return response

        # for other request types, return most useful data
        content_type = response.headers.get("content-type").strip().lower()
        if re.match(r"application\/(vnd\.geo\+)?json", content_type):
            return response.json()
        if re.match(r"text\/csv", content_type):
            csv_stream = StringIO(response.text)
            return list(csv.reader(csv_stream))
        if re.match(r"application\/rdf\+xml", content_type):
            return response.content
        if re.match(r"text\/plain", content_type):
            try:
                return json.loads(response.text)
            except ValueError:
                return response.text

        raise Exception(f"Unknown response format: {content_type}")

    def close(self) -> None:
        """
        Close the session.
        """
        self.session.close()
