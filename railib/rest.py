# Copyright 2021 RelationalAI, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Low level HTTP interface to the RelationalAI REST API."""

import json
import logging
from os import path
from urllib.error import URLError
from urllib.parse import urlencode, urlsplit, quote
from urllib.request import Request, urlopen

from .__init__ import __version__
from .credentials import (
    AccessToken,
    Credentials,
    ClientCredentials,
)

__all__ = ["Context", "get", "put", "post", "request"]


ACCESS_KEY_TOKEN_KEY = "access_token"
CLIENT_ID_KEY = "client_id"
CLIENT_SECRET_KEY = "client_secret"
AUDIENCE_KEY = "audience"
GRANT_TYPE_KEY = "grant_type"
CLIENT_CREDENTIALS_KEY = "client_credentials"
EXPIRES_IN_KEY = "expires_in"
SCOPE = "scope"

# logger
logger = logging.getLogger(__package__)


# Context contains the state required to make rAI REST API calls.
class Context(object):
    def __init__(self, region: str = None, credentials: Credentials = None, retries: int = 0):
        if retries < 0:
            raise ValueError("Retries must be a non-negative integer")

        self.region = region or "us-east"
        self.credentials = credentials
        self.service = "transaction"
        self.retries = retries


# Answers if the keys of the passed dict contain a case insensitive match
# for the given term.
def _contains_insensitive(items: dict, term: str) -> bool:
    term = term.casefold()
    for item in items:
        item = item.casefold()
        if term == item:
            return True
    return False


# Fill in any missing headers.
def _default_headers(url: str, headers: dict = None) -> dict:
    headers = headers or {}
    if not _contains_insensitive(headers, "accept"):
        headers["Accept"] = "application/json"
    if not _contains_insensitive(headers, "content-type"):
        headers["Content-Type"] = "application/json"
    if not _contains_insensitive(headers, "host"):
        headers["Host"] = _get_host(url)
    if not _contains_insensitive(headers, "user-agent"):
        headers["User-Agent"] = _default_user_agent()
    return headers


def _default_user_agent() -> str:
    return f"rai-sdk-python/{__version__}"


def _encode(data) -> bytes:
    if not data:
        return data
    if not isinstance(data, str):
        data = json.dumps(data)
    return data.encode("utf8")


def _encode_path(path: str) -> str:
    # double encoding as per AWS v4
    return quote(quote(path))


# Returns an urlencoded query string.
# Note: the signing algo below **requires** query params to be sorted.
def _encode_qs(kwargs: dict) -> str:
    args = sorted([(k, v) for k, v in kwargs.items()])
    return urlencode(args)


# Retrieve the hostname from the given url.
def _get_host(url: str) -> str:
    return urlsplit(url).netloc.split(":")[0]


def _print_request(req: Request, level=0):
    if level <= 0:
        return
    if level > 0:
        print(f"{req.method} {req.full_url}")
        if level > 1:
            for k, v in req.headers.items():
                print(f"{k}: {v}")
            if req.data:
                print(json.dumps(json.loads(req.data.decode("utf8")), indent=2))


def _cache_file() -> str:
    return path.join(path.expanduser('~'), '.rai', 'tokens.json')


# Read oauth cache
def _read_cache() -> dict:
    try:
        with open(_cache_file(), 'r') as cache:
            return json.loads(cache.read())
    except Exception as e:
        logger.warning(f'Failed to read token cache {_cache_file()}: {e}')
        return {}


# Read access token from cache
def _read_token_cache(creds: ClientCredentials) -> AccessToken:
    try:
        cache = _read_cache()
        return AccessToken(**cache[creds.client_id])
    except Exception:
        return None


# write access token to cache
def _write_token_cache(creds: ClientCredentials):
    try:
        cache = _read_cache()
        cache[creds.client_id] = creds.access_token

        with open(_cache_file(), 'w') as f:
            f.write(json.dumps(cache, default=vars))
    except Exception as e:
        logger.warning(f'Failed to write to token cache {_cache_file()}: {e}')


# Returns the current access token if valid, otherwise requests new token.
def _get_access_token(ctx: Context, url: str) -> AccessToken:
    creds = ctx.credentials
    assert isinstance(creds, ClientCredentials)
    if creds.access_token is None or creds.access_token.is_expired():
        creds.access_token = _read_token_cache(creds)
        if creds.access_token is None or creds.access_token.is_expired():
            creds.access_token = _request_access_token(ctx, url)
            _write_token_cache(creds)
    return creds.access_token.access_token


def _request_access_token(ctx: Context, url: str) -> AccessToken:
    creds = ctx.credentials
    assert isinstance(creds, ClientCredentials)
    # ensure the audience contains the protocol scheme
    audience = ctx.audience or f"https://{_get_host(url)}"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Host": _get_host(creds.client_credentials_url),
        "User-Agent": _default_user_agent(),
    }
    body = {
        CLIENT_ID_KEY: creds.client_id,
        CLIENT_SECRET_KEY: creds.client_secret,
        AUDIENCE_KEY: audience,
        GRANT_TYPE_KEY: CLIENT_CREDENTIALS_KEY,
    }
    data = _encode(body)
    req = Request(
        method="POST",
        url=creds.client_credentials_url,
        headers=headers,
        data=data,
    )
    _print_request(req)
    with _urlopen_with_retry(req, ctx.retries) as rsp:
        result = json.loads(rsp.read())
        token = result.get(ACCESS_KEY_TOKEN_KEY, None)

        if token is not None:
            expires_in = result.get(EXPIRES_IN_KEY, None)
            scope = result.get(SCOPE, None)
            return AccessToken(token, scope, expires_in)

    raise Exception("failed to get the access token")


# Authenticate the request by signing or adding access token.
def _authenticate(ctx: Context, req: Request) -> Request:
    creds = ctx.credentials
    if creds is None:
        return req
    if isinstance(creds, ClientCredentials):
        access_token = _get_access_token(ctx, req.full_url)
        req.headers["authorization"] = f"Bearer {access_token}"
        return req
    raise Exception("unknown credential type")


# Issues an HTTP request and retries if failed due to URLError.
def _urlopen_with_retry(req: Request, retries: int = 0):
    if retries < 0:
        raise ValueError("Retries must be a non-negative integer")

    attempts = retries + 1

    for attempt in range(attempts):
        try:
            return urlopen(req)
        except (URLError, ConnectionError) as e:
            logger.warning(f"URL/Connection error occured {req.full_url} (attempt {attempt + 1}/{attempts}). Error message: {str(e)}")
            
            if attempt == attempts - 1:
                logger.error(f"Failed to connect to {req.full_url} after {attempts} attempt{'s' if attempts > 1 else ''}")
                raise e


# Issues an RAI REST API request, and returns response contents if successful.
def request(ctx: Context, method: str, url: str, headers={}, data=None, **kwargs):
    headers = _default_headers(url, headers)
    if kwargs:
        url = f"{url}?{_encode_qs(kwargs)}"
    data = _encode(data)
    req = Request(method=method, url=url, headers=headers, data=data)
    req = _authenticate(ctx, req)
    _print_request(req)
    rsp = _urlopen_with_retry(req, ctx.retries)

    # logging
    content_type = headers["Content-Type"] if "Content-Type" in headers else ""
    agent = headers["User-Agent"] if "User-Agent" in headers else ""
    request_id = rsp.headers["X-Request-ID"] if "X-Request-ID" in rsp.headers else ""
    logger.debug(f"{rsp._method} HTTP/{rsp.version} {content_type} {rsp.url} {rsp.status} {agent} {request_id}")
    return rsp


def delete(ctx: Context, url: str, data, headers={}, **kwargs):
    return request(ctx, "DELETE", url, headers=headers, data=data, **kwargs)


def get(ctx: Context, url: str, headers={}, **kwargs):
    return request(ctx, "GET", url, headers=headers, **kwargs)


def put(ctx: Context, url: str, data, headers={}, **kwargs):
    return request(ctx, "PUT", url, headers=headers, data=data, **kwargs)


def post(ctx: Context, url: str, data, headers={}, **kwargs):
    return request(ctx, "POST", url, headers=headers, data=data, **kwargs)


def patch(ctx: Context, url: str, data, headers={}, **kwargs):
    return request(ctx, "PATCH", url, headers=headers, data=data, **kwargs)
