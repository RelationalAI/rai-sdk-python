# Answers if the given list of strings contains a case insensitive match
# for the given term.
import json
from urllib.parse import urlsplit
from .__init__ import __version__


def _contains_insensitive(items: list, term: str) -> bool:
    term = term.casefold()
    for item in items:
        item = item.casefold()
        if term == item:
            return True
    return False


# Retrieve the hostname from the given url.
def _get_host(url: str) -> str:
    return urlsplit(url).netloc.split(':')[0]


# Fill in any missing headers.
def default_headers(url: str, headers: dict) -> None:
    if not _contains_insensitive(headers, "accept"):
        headers["Accept"] = "application/json"
    if not _contains_insensitive(headers, "content-type"):
        headers["Content-Type"] = "application/json"
    if not _contains_insensitive(headers, "host"):
        headers["Host"] = _get_host(url)
    if not _contains_insensitive(headers, "user-agent"):
        headers["User-Agent"] = f"rai-sdk-python/{__version__}"
        # "OpenAPI-Generator/1.0.0/python"


def encode(data) -> str:
    if not data:
        return data
    if not isinstance(data, str):
        data = json.dumps(data)
    return data.encode("utf8")