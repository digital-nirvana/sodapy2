import csv
import json
import logging
import os
import re
from io import StringIO
from typing import Any, Generator, Union
from urllib.parse import urlunsplit

import requests
import requests.adapters

import sodapy2.utils as utils
from sodapy2.constants import Formats, SodaApiEndpoints


class Socrata:
    """
    The main class that interacts with the SODA API. Sample usage:
        from sodapy2 import Socrata
        client = Socrata("opendata.socrata.com")
    """

    def __init__(
        self,
        domain: str,
        app_token: str = "",
        session_adapter: Union[requests.adapters.BaseAdapter, None] = None,
        timeout: Union[int, float] = 10,
    ):
        """
        Initialize an instance of Socrata.

        Simple requests are possible without an app token but such requests will be rate-limited.
        See https://dev.socrata.com/docs/app-tokens for information about application tokens.

        Args:
            domain: the domain you wish you to access
            app_token: your Socrata application token (optional)
        """
        if not domain:
            raise ValueError("Arg `domain` must not be null.")
        self.domain = domain

        if not isinstance(timeout, (int, float)):
            raise TypeError("Arg `timeout` must be numeric.")
        self.timeout = timeout

        self.proto = "http+mock" if os.environ.get("PYTEST_CURRENT_TEST") else "https"

        self.session = requests.Session()
        if not app_token:
            logging.warning("Requests made without an app_token will be" " subject to strict throttling limits.")
        else:
            self.session.headers.update({"X-App-token": app_token})

        if session_adapter:
            self.session.mount(prefix=f"{self.proto}://", adapter=session_adapter)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def get_datasets(self, limit: int = 0, offset: int = 0, order: str = "", **kwargs):
        """
        Return the list of datasets associated with a particular domain.
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
            params.append(("limit", str(limit)))
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

        results, _ = self._perform_request(
            "get", SodaApiEndpoints.DISCOVERY.endpoint, params=params + [("offset", offset)]
        )
        num_results = results["resultSetSize"]
        # no more results to fetch, or limit reached
        if limit >= num_results or limit == len(results["results"]) or num_results == len(results["results"]):
            return results["results"]

        if limit != 0:
            raise Exception(
                f"Unexpected number of results returned from endpoint. Expected {limit}, got {len(results['results'])}."
            )

        # get all remaining results
        all_results = results["results"]
        while len(all_results) != num_results:
            offset += len(results["results"])
            results, _ = self._perform_request(
                "get", SodaApiEndpoints.DISCOVERY.endpoint, params=params + [("offset", offset)]
            )
            all_results.extend(results["results"])

        return all_results

    def get(self, dataset_id: str, content_type: str = "json", **params) -> Union[list[list[str]], str]:
        """
        Fetch data for a given dataset.

        Args:
            dataset_id: The identifier of the desired dataset.
            content_type: The desired results format.

            Optionally, specify a kwarg-style parameters to filter results:
                select : the set of columns to be returned; Default: *
                where : filters the rows to be returned.
                order : specifies the order of results; Default: non-deterministic ordering.
                group : column to group results on for aggregate queries.
                limit : max number of results to return; Default: 1000.
                offset : offset, used for paging; Default: 0.
                q : performs a full text search for a value.
                query : full SoQL query string, all as one parameter.
                exclude_system_fields : defaults to true. If set to false, the
                    response will include system fields (:id, :created_at, and
                    :updated_at)

        More information about the SoQL parameters can be found at the official
        docs:
            https://dev.socrata.com/docs/queries

        More information about system fields can be found here:
            https://dev.socrata.com/docs/system-fields
        """
        if not dataset_id:
            raise ValueError("dataset_id must not be null")
        if content_type.upper() not in Formats.__members__:
            content_types = [type.lower() for type in list(Formats.__members__.keys())]
            raise ValueError(f"content_type must be one of: {content_types}")

        resource = f"{SodaApiEndpoints.DATASET.endpoint}/{dataset_id}"
        headers = {"Accept": Formats[content_type.upper()].mimetype}

        # SoQL parameters. Initialize all as "None" because null values will be pruned before sending the request.
        params = {
            "$select": params.pop("select", None),
            "$where": params.pop("where", None),
            "$order": params.pop("order", None),
            "$group": params.pop("group", None),
            "$limit": params.pop("limit", None),
            "$offset": params.pop("offset", None),
            "$q": params.pop("q", None),
            "$query": params.pop("query", None),
            "$$exclude_system_fields": params.pop("exclude_system_fields", None),
        }
        params = utils.prune_empty_values(params)

        response, _ = self._perform_request("get", resource, headers=headers, params=params)
        return response

    def get_all(self, dataset_id: str, content_type: str = "json", **params) -> Generator[Any, Any, Any]:
        """
        Fetch data for a given dataset and paginate over all results.

        Args:
            dataset_id: The identifier of the desired dataset.
            content_type: The desired results format.

            See optional params in `get()`.

        Returns:
            Generator of results.
        """
        # Set these values specifically because they're used to control paging.
        params["order"] = params.get("order", ":id")
        params["offset"] = params.get("offset", 0)
        params["limit"] = params.get("limit", 1000)  # 1000 is the default SODA API limit.

        response = self.get(dataset_id=dataset_id, content_type=content_type, **params)
        while response:
            for item in response:
                yield item
            if len(response) < params["limit"]:  # There are no more results.
                return
            params["offset"] += params["limit"]
            response = self.get(dataset_id=dataset_id, content_type=content_type, **params)

    def get_metadata(self, dataset_id: str = "") -> dict:
        """
        Retrieve the metadata for a particular dataset.

        If no dataset_id is given, all metadata will be returned.

        Args:
            dataset_id: The identifier of the desired dataset.

        Returns:
            The dataset's metadata.
        """
        resource = (
            f"{SodaApiEndpoints.METADATA.endpoint}/{dataset_id}" if dataset_id else SodaApiEndpoints.METADATA.endpoint
        )
        metadata, _ = self._perform_request(method="get", resource=resource)
        return metadata

    def _perform_request(self, method: str, resource: str, **kwargs) -> tuple:
        """
        Utility method that performs all requests.

        Args:
            method: the HTTP method to use, e.g. get, post.
            resource: the resource to request.

        Returns:
            The response body and content type.

        """
        supported_http_methods = frozenset(["get"])
        if method not in supported_http_methods:
            raise Exception(f"Unknown HTTP request method. Supported methods are: {supported_http_methods}")

        uri = urlunsplit((self.proto, self.domain, resource, None, None))
        kwargs["timeout"] = self.timeout
        response = getattr(self.session, method)(uri, **kwargs)

        if response.status_code not in (200, 202):
            utils.raise_for_status(response)

        content_type = response.headers.get("content-type").strip().lower()
        if re.match(r"application\/(vnd\.geo\+)?json", content_type):
            return_val = (response.json(), content_type)
        elif content_type == "text/csv":
            return_val = (list(csv.reader(StringIO(response.text))), content_type)
        elif content_type == "application/rdf+xml":
            return_val = (response.content, content_type)
        elif content_type == "text/plain":
            try:
                return_val = (json.loads(response.text), content_type)
            except ValueError:
                return_val = (response.text, content_type)
        else:
            raise Exception(f"Unknown response format: {content_type}")
        return return_val

    def close(self) -> None:
        """Close the session."""
        self.session.close()
