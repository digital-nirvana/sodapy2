from enum import Enum


class Formats(Enum):
    CSV = ("csv", "text/csv; charset=utf-8")
    JSON = ("json", "application/json; charset=utf-8")
    RDFXML = ("rdf", "application/rdf+xml; charset=utf-8")
    XML = ("xml", "text/xml; charset=utf-8")

    @property
    def extension(self) -> str:
        return self.value[0]

    @property
    def mimetype(self) -> str:
        return self.value[1]


class SodaApiEndpoints(Enum):
    DISCOVERY = "/api/catalog/v1"
    DATASET = "/resource"
    METADATA = "/api/views/metadata/v1"

    @property
    def endpoint(self) -> str:
        return self.value
