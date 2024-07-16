import csv
import json
import logging
import re
from io import StringIO
from typing import Any, Generator, Union
from urllib.parse import urlunsplit

import requests
import requests.adapters

import sodapy2.utils as utils
from sodapy2 import __version__
from sodapy2.constants import Formats, SodaApiEndpoints


class Socrata:
    """
    The main class that interacts with the SODA API. Sample usage:
        from sodapy2 import Socrata
        client = Socrata("opendata.socrata.com")
    """

    proto = "https"

    def __init__(
        self,
        domain: str,
        app_token: str = "",
        session_adapter: Union[requests.adapters.BaseAdapter, None] = None,
        timeout: Union[int, float] = 10,
        user_agent: str = f"sodapy2/{__version__.__version__}",
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

        self.session = requests.Session()
        self.session.headers["User-Agent"] = user_agent
        if not app_token:
            logging.warning("Requests made without an app_token will be subject to strict throttling limits.")
        else:
            self.session.headers.update({"X-App-token": app_token})

        if session_adapter:
            self.session.mount(prefix=f"{self.proto}://", adapter=session_adapter)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def get_datasets(self, filters: dict = {}, **params) -> dict:
        """
        Return the list of datasets associated with the session domain.

        This method uses the Socrata Discovery API: https://dev.socrata.com/docs/other/discovery

        Args:
            filters: A dictionary of filters to apply to the search.
                approval_status (str): filter by current status in the approval pipeline;
                    'approved', 'not_ready', 'pending', 'rejected'.
                attribution (str) : filter by organization.
                categories (list) : filter by dataset categories.
                column_names (list) : column names that must be present in the tabular datasets.
                derived (bool) : filter by derived datasets (True) or only those from which
                    other datasets were derived (False).
                derived_from (str) : return datasets that were derived from a given dataset ID.
                domains (list) : additional domains to search.
                explicitly_hidden (bool) : filter datasets that have been explicitly hidden.
                for_user (str) : return datasets owned by a given user ID.
                ids (list): filter by a list of dataset IDs.
                license (str) : filter by a specific license.
                tags (list) : filter by a list of tags.
                min_should_match (str) : string specifying the number of words from 'q'.
                    that should match. Refer to Elasticsearch docs for the format;
                    Default: '3<60%', i.e. 60% of the terms must match (or all if <=3 results).
                provenance (str) : filter by provenance; 'official' or 'community'.
                public (bool) : filter by public (True) or private (False) datasets.
                published (bool) : filter by published status; published (True) | unpublished (False).
                only (list) : list of logical types to return;
                    'api', 'calendar', 'chart', 'datalens', 'dataset', 'federated_href', 'file',
                    'filter', 'form', 'href', 'link', 'map', 'measure', 'story', 'visualization'
                q (str) : text query that will be used by Elasticsearch to match results.
                shared_to (list) : return datasets that have been shared with the given 'user' or 'team' IDs.
                    Alternatively, 'site' will mean anyone on the domain.
                    Note that you may only specify yourself or a team that you are on.
                    Also note that if you search for assets shared to you,
                    assets owned by you might be not be returned.
                visibility (str) : filter by dataset visibility; 'open' or 'internal'.

            Optionally, specify kwarg-style parameters to sort or paginate results:
                limit (int): max number of results to return; Default: 1000.
                offset (int) : offset, used for paging; Default: 0.
                order : field to sort on, optionally with ' ASC' or ' DESC' suffix; Default: by relevance.

        Returns:
            A list of datasets and their metadata.
        """
        # These filters can be passed multiple times; this function expects an iterable for them.
        filter_multiple = frozenset(
            [
                "categories",
                "column_names",
                "domains",
                "ids",
                "only",
                "shared_to",
                "tags",
            ]
        )
        # These filters may only be specified once.
        filter_single = frozenset(
            [
                "approval_status",
                "attribution",
                "derived",
                "derived_from",
                "explicitly_hidden",
                "for_user",
                "license",
                "min_should_match",
                "provenance",
                "public",
                "published",
                "q",
                "visibility",
            ]
        )
        all_filters = filter_multiple.union(filter_single)
        for filter in filters:
            if filter not in all_filters:
                raise TypeError("Unknown filter %s" % filter)
        params.update(filters)

        order_fields = frozenset(
            [
                "createdAt",
                "dataset_id",
                "datatype",
                "domain_category",
                "name",
                "owner",
                "page_views_total",
                "page_views_last_month",
                "page_views_last_week",
                "relevance",  # Default for Socrata Discovery API.
                "updatedAt",
            ]
        )
        params["limit"] = params.get("limit", 1000)
        if params.get("order") and params.get("order") not in order_fields:
            raise ValueError(f"Invalid order parameter. Must be one of {order_fields}")

        # TODO: custom domain-specific metadata
        # https://socratadiscovery.docs.apiary.io/reference/0/find-by-domain-specific-metadata

        response, _ = self._perform_request("get", SodaApiEndpoints.DISCOVERY.endpoint, params=params)
        return response

    def get(self, dataset_id: str, content_type: str = "json", **params) -> Union[list[list[str]], str]:
        """
        Fetch data for a given dataset.

        Args:
            content_type: The desired results format.
            dataset_id: The identifier of the desired dataset.

            Optionally, specify kwarg-style parameters to filter results:
                exclude_system_fields : defaults to true. If set to false, the
                    response will include system fields (:id, :created_at, and
                    :updated_at)
                group : column to group results on for aggregate queries.
                limit : max number of results to return; Default: 1000.
                offset : offset, used for paging; Default: 0.
                order : field to sort on, optionally with ' ASC' or ' DESC' suffix; Default: ":id".
                select : the set of columns to be returned; Default: *
                q : performs a full text search for a value.
                query : full SoQL query string, all as one parameter.
                where : filters the rows to be returned.

        More information about the SoQL parameters can be found at the official
        docs:
            https://dev.socrata.com/docs/queries

        More information about system fields can be found here:
            https://dev.socrata.com/docs/system-fields

        Returns:
            Dataset results.

        """
        if not dataset_id:
            raise ValueError("dataset_id must not be null")
        if content_type.upper() not in Formats.__members__:
            content_types = [type.lower() for type in list(Formats.__members__.keys())]
            raise ValueError(f"content_type must be one of: {content_types}")

        # By default, Socrata returns data in a non-deterministic order which messes up pagination using offset/limit.
        # Avoid letting the caller shoot themselves in the foot by setting a default order. But if the caller knows
        # this and wants non-deterministic ordering, don't prevent them from setting order = "".
        if "order" not in params:
            params["order"] = ":id"

        headers = {"Accept": Formats[content_type.upper()].mimetype}
        params = {f"${k}": v for k, v in params.items()}  # Prepend a $ to the SoQL parameters, per the SODA API spec.
        resource = f"{SodaApiEndpoints.DATASET.endpoint}/{dataset_id}"

        response, _ = self._perform_request("get", resource, headers=headers, params=params)
        return response

    def get_all(self, dataset_id: str, content_type: str = "json", **params) -> Generator[Any, Any, Any]:
        """
        Fetch all data for a given dataset by automatically paginating over the `get()` method.

        Args:
            content_type: The desired results format.
            dataset_id: The identifier of the desired dataset.
            params: Optional kwarg-style parameters. See optional params in `get()`.

        Returns:
            Generator of results.
        """
        if params.get("offset") or params.get("limit"):
            logging.warning('Ignoring "offset" and/or "limit" parameters for get_all() query.')
        params["order"] = params.get("order", ":id")
        params["offset"] = 0
        params["limit"] = 1000

        response = self.get(dataset_id=dataset_id, content_type=content_type, **params)
        while response:
            for item in response:
                yield item
            if len(response) < params["limit"]:  # There are no more results.
                return
            params["offset"] += params["limit"]
            response = self.get(dataset_id=dataset_id, content_type=content_type, **params)

    def get_metadata(self, dataset_id: str = "") -> Union[dict, list]:
        """
        Retrieve the metadata for a particular domain dataset.

        If no dataset_id is given, all the domain's metadata will be returned.

        Args:
            dataset_id: The identifier of the desired dataset.

        Returns:
            The dataset's metadata as a dict. If no dataset_id was given, this method returns a list.
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

        if kwargs.get("params"):
            kwargs["params"] = utils.prune_empty_values(kwargs["params"])
        kwargs["timeout"] = self.timeout
        response = getattr(self.session, method)(uri, **kwargs)
        if response.status_code not in (200, 202):
            utils.raise_for_status(response)

        content_type = response.headers.get("content-type").strip().lower()
        if re.match(r"application\/(vnd\.geo\+)?json", content_type):
            return_val = (response.json(), content_type)
        elif content_type == Formats.CSV.mimetype:
            return_val = (list(csv.reader(StringIO(response.text))), content_type)
        elif content_type == Formats.RDFXML.mimetype:
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
