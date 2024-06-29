<!-- 
TODO: Reinstate all of this

[![PyPI version](https://badge.fury.io/py/sodapy2.svg)](http://badge.fury.io/py/sodapy2) [![Build Status](https://travis-ci.com/xmunoz/sodapy2.svg?branch=master)](https://travis-ci.com/xmunoz/sodapy2) [![Code Coverage](https://codecov.io/github/xmunoz/sodapy2/coverage.svg?branch=master)](https://codecov.io/github/xmunoz/sodapy2) -->
<!-- TODO ^^^^ -->

# sodapy2

DISCLAIMER: This project is not ready for production use. I am working to get it into such a state. Updates coming soon!

sodapy2 is a Python client for the [Socrata Open Data API](https://dev.socrata.com/).

## Installation
<!-- You can install with `pip install sodapy2`. -->
<!-- Not in PyPi yet. ^^^ -->

If you want to install from source, then clone this repository and run `python setup.py install` from the project root.

## Compatibility

`sodapy2` is compatible with Python >=3.6.

## Documentation

This library offers data discovery and reading tools. It does not offer write or delete capabilities. For that, use [Socrata's native Python package](https://github.com/socrata/socrata-py).

The [official Socrata Open Data API docs](http://dev.socrata.com/) provide thorough documentation of the available methods, as well as [other client libraries](https://dev.socrata.com/libraries/). A quick list of eligible domains to use with this API is available via the [Socrata Discovery API](https://socratadiscovery.docs.apiary.io/#reference/0/count-by-domain/count-by-domain?console=1) or [Socrata's Open Data Network](https://www.opendatanetwork.com/).

## Examples

There are some [jupyter](https://jupyter.org/) notebooks in the [examples directory](examples) with usage examples of sodapy2 in action.

## Interface

### Table of Contents

- [client](#client)
- [`datasets`](#datasetslimit0-offset0)
- [`get`](#getdataset_identifier-content_typejson-kwargs)
- [`get_all`](#get_alldataset_identifier-content_typejson-kwargs)
- [`get_metadata`](#get_metadatadataset_identifier-content_typejson)
- [`download_attachments`](#download_attachmentsdataset_identifier-content_typejson-download_dirsodapy2_downloads)
- [`close`](#close)

### client

Import the library and set up a connection to get started.

    >>> from sodapy2 import Socrata
    >>> client = Socrata(
            "sandbox.demo.socrata.com",
            "FakeAppToken",
            username="fakeuser@somedomain.com",
            password="mypassword",
            timeout=10
        )

`username` and `password` are only required for creating or modifying data. An application token isn't strictly required (can be `None`), but queries executed from a client without an application token will be subjected to strict throttling limits. You may want to increase the `timeout` seconds when making large requests. To create a bare-bones client:

    >>> client = Socrata("sandbox.demo.socrata.com", None)

A client can also be created with a context manager to obviate the need for teardown:

    >>> with Socrata("sandbox.demo.socrata.com", None) as client:
    >>>    # do some stuff

The client, by default, makes requests over HTTPS. To modify this behavior, or to make requests through a proxy, take a look [here](https://github.com/digital-nirvana/sodapy2/issues/31#issuecomment-302176628).

### datasets(limit=0, offset=0)

Retrieve datasets associated with a particular domain. The optional `limit` and `offset` keyword args can be used to retrieve a subset of the datasets. By default, all datasets are returned.

    >>> client.datasets()
    [{"resource" : {"name" : "Approved Building Permits", "id" : "msk6-43c6", "parent_fxf" : null, "description" : "Data of approved building/construction permits",...}, {resource : {...}}, ...]

### get(dataset_identifier, content_type="json", **kwargs)

Retrieve data from the requested resources. Filter and query data by field name, id, or using [SoQL keywords](https://dev.socrata.com/docs/queries/).

    >>> client.get("nimj-3ivp", limit=2)
	[{u'geolocation': {u'latitude': u'41.1085', u'needs_recoding': False, u'longitude': u'-117.6135'}, u'version': u'9', u'source': u'nn', u'region': u'Nevada', u'occurred_at': u'2012-09-14T22:38:01', u'number_of_stations': u'15', u'depth': u'7.60', u'magnitude': u'2.7', u'earthquake_id': u'00388610'}, {...}]

	>>> client.get("nimj-3ivp", where="depth > 300", order="magnitude DESC", exclude_system_fields=False)
	[{u'geolocation': {u'latitude': u'-15.563', u'needs_recoding': False, u'longitude': u'-175.6104'}, u'version': u'9', u':updated_at': 1348778988, u'number_of_stations': u'275', u'region': u'Tonga', u':created_meta': u'21484', u'occurred_at': u'2012-09-13T21:16:43', u':id': 132, u'source': u'us', u'depth': u'328.30', u'magnitude': u'4.8', u':meta': u'{\n}', u':updated_meta': u'21484', u'earthquake_id': u'c000cnb5', u':created_at': 1348778988}, {...}]

    >>> client.get("nimj-3ivp/193", exclude_system_fields=False)
    {u'geolocation': {u'latitude': u'21.6711', u'needs_recoding': False, u'longitude': u'142.9236'}, u'version': u'C', u':updated_at': 1348778988, u'number_of_stations': u'136', u'region': u'Mariana Islands region', u':created_meta': u'21484', u'occurred_at': u'2012-09-13T11:19:07', u':id': 193, u'source': u'us', u'depth': u'300.70', u'magnitude': u'4.4', u':meta': u'{\n}', u':updated_meta': u'21484', u':position': 193, u'earthquake_id': u'c000cmsq', u':created_at': 1348778988}

    >>> client.get("nimj-3ivp", region="Kansas")
	[{u'geolocation': {u'latitude': u'38.10', u'needs_recoding': False, u'longitude': u'-100.6135'}, u'version': u'9', u'source': u'nn', u'region': u'Kansas', u'occurred_at': u'2010-09-19T20:52:09', u'number_of_stations': u'15', u'depth': u'300.0', u'magnitude': u'1.9', u'earthquake_id': u'00189621'}, {...}]

### get_all(dataset_identifier, content_type="json", **kwargs)

Read data from the requested resource, paginating over all results. Accepts the same arguments as [`get()`](#getdataset_identifier-content_typejson-kwargs). Returns a generator.

    >>> client.get_all("nimj-3ivp")
	<generator object Socrata.get_all at 0x7fa0dc8be7b0>

    >>> for item in client.get_all("nimj-3ivp"):
	...     print(item)
    ...
    {'geolocation': {'latitude': '-15.563', 'needs_recoding': False, 'longitude': '-175.6104'}, 'version': '9', ':updated_at': 1348778988, 'number_of_stations': '275', 'region': 'Tonga', ':created_meta': '21484', 'occurred_at': '2012-09-13T21:16:43', ':id': 132, 'source': 'us', 'depth': '328.30', 'magnitude': '4.8', ':meta': '{\n}', ':updated_meta': '21484', 'earthquake_id': 'c000cnb5', ':created_at': 1348778988}
    ...

    >>> import itertools
    >>> items = client.get_all("nimj-3ivp")
    >>> first_five = list(itertools.islice(items, 5))
    >>> len(first_five)
    5

### get_metadata(dataset_identifier, content_type="json")

Retrieve the metadata associated with a particular dataset.

    >>> client.get_metadata("nimj-3ivp")
    {"newBackend": false, "licenseId": "CC0_10", "publicationDate": 1436655117, "viewLastModified": 1451289003, "owner": {"roleName": "administrator", "rights": [], "displayName": "Brett", "id": "cdqe-xcn5", "screenName": "Brett"}, "query": {}, "id": "songs", "createdAt": 1398014181, "category": "Public Safety", "publicationAppendEnabled": true, "publicationStage": "published", "rowsUpdatedBy": "cdqe-xcn5", "publicationGroup": 1552205, "displayType": "table", "state": "normal", "attributionLink": "http://foo.bar.com", "tableId": 3523378, "columns": [], "metadata": {"rdfSubject": "0", "renderTypeConfig": {"visible": {"table": true}}, "availableDisplayTypes": ["table", "fatrow", "page"], "attachments": ... }}

### download_attachments(dataset_identifier, content_type="json", download_dir="~/sodapy2_downloads")

Download all attachments associated with a dataset. Return a list of paths to the downloaded files.

    >>> client.download_attachments("nimj-3ivp", download_dir="~/Desktop")
        ['/Users/foo/Desktop/nimj-3ivp/FireIncident_Codes.PDF', '/Users/foo/Desktop/nimj-3ivp/AccidentReport.jpg']

### close()

Close the session when you're finished.

	>>> client.close()

## How to contribute

See [CONTRIBUTING.md](https://github.com/digital-nirvana/sodapy2/blob/main/CONTRIBUTING.md).

## Acknowledgements

sodapy2 is a continuation of the sodapy project by Christina Munoz.
(https://github.com/xmunoz/sodapy)


