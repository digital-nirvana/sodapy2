from sodapy2.constants import Formats, SodaApiEndpoints


def test_formats_csv_extension():
    assert Formats.CSV.extension == "csv"


def test_formats_csv_mimetype():
    assert Formats.CSV.mimetype == "text/csv; charset=utf-8"


def test_formats_json_extension():
    assert Formats.JSON.extension == "json"


def test_formats_json_mimetype():
    assert Formats.JSON.mimetype == "application/json; charset=utf-8"


def test_formats_rdfxml_extension():
    assert Formats.RDFXML.extension == "rdf"


def test_formats_rdfxml_mimetype():
    assert Formats.RDFXML.mimetype == "application/rdf+xml; charset=utf-8"


def test_formats_xml_extension():
    assert Formats.XML.extension == "xml"


def test_formats_xml_mimetype():
    assert Formats.XML.mimetype == "text/xml; charset=utf-8"


def test_sodaapi_discovery_endpoint():
    assert SodaApiEndpoints.DISCOVERY.endpoint == "/api/catalog/v1"


def test_sodaapi_dataset_endpoint():
    assert SodaApiEndpoints.DATASET.endpoint == "/resource"


def test_sodaapi_metadata_endpoint():
    assert SodaApiEndpoints.METADATA.endpoint == "/api/views/metadata/v1"
