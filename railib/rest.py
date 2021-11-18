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

from abc import ABC
import ed25519
import base64
from datetime import datetime
import hashlib
import json
from pprint import pprint
from urllib.parse import urlencode, urlsplit, quote
from urllib.request import Request, urlopen
import time

from .__init__ import __version__
from .credentials import AccessKeyCredentials, AccessToken, Credentials, ClientCredentials

__all__ = ["Context", "get", "put", "post", "request"]


ACCESS_KEY_TOKEN_KEY = "access_token"
CLIENT_ID_KEY = "client_id"
CLIENT_SECRET_KEY = "client_secret"
AUDIENCE_KEY = "audience"
GRANT_TYPE_KEY = "grant_type"
CLIENT_CREDENTIALS_KEY = "client_credentials"
EXPIRES_IN_KEY = "expires_in"


_empty = bytes('', encoding='utf8')


# Context contains the state required to make rAI REST API calls.
class Context(object):
    def __init__(self, region: str = None, credentials: Credentials = None):
        self.region = region or "us-east"
        self.credentials = credentials
        self.service = "transaction"


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
    args = [(k, v) for k, v in kwargs.items()]
    args.sort()
    return urlencode(args)


# Retrieve the hostname from the given url.
def _get_host(url: str) -> str:
    return urlsplit(url).netloc.split(':')[0]


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


# Returns the current access token if valid, otherwise requests new token.
def _get_access_token(ctx: Context, url: str) -> AccessToken:
    creds = ctx.credentials
    assert type(creds) == ClientCredentials
    if creds.access_token is None or creds.access_token.is_expired():
        creds.access_token = _request_access_token(ctx, url)
    return creds.access_token.token


def _request_access_token(ctx: Context, url: str) -> AccessToken:
    creds = ctx.credentials
    assert type(creds) == ClientCredentials
    # ensure the audience contains the protocol scheme
    audience = f"https://{_get_host(url)}"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Host": _get_host(creds.client_credentials_url),
        "User-Agent": _default_user_agent()}
    body = {
        CLIENT_ID_KEY: creds.client_id,
        CLIENT_SECRET_KEY: creds.client_secret,
        AUDIENCE_KEY: audience,
        GRANT_TYPE_KEY: CLIENT_CREDENTIALS_KEY}
    data = _encode(body)
    req = Request(method="POST", url=creds.client_credentials_url,
                  headers=headers, data=data)
    _print_request(req)
    with urlopen(req) as rsp:
        result = json.loads(rsp.read())
        token = result.get(ACCESS_KEY_TOKEN_KEY, None)
        if token is not None:
            expires_in = result.get(EXPIRES_IN_KEY, None)
            return AccessToken(token, expires_in)
    raise Exception("failed to get the access token")


# Implement RAI API key authentication by signing the request and adding the
# required authorization header.
def _sign(ctx: Context, req: Request) -> None:
    assert type(ctx.credentials) == AccessKeyCredentials

    ts = datetime.utcnow()

    # ISO8601 date/time strings for time of request
    signature_date = ts.strftime("%Y%m%dT%H%M%SZ")
    scope_date = ts.strftime("%Y%m%d")

    # Authentication scope
    scope = f"{scope_date}/{ctx.region}/{ctx.service}/rai01_request"

    # SHA256 hash of content
    content = req.data or _empty
    content_hash = hashlib.sha256(content).hexdigest()

    # Include "x-rai-date" in signed headers
    req.headers["x-rai-date"] = signature_date

    # Sort and lowercase headers to produce a canonical form
    canonical_headers = [f"{k.lower()}:{v.strip()}" for k,
                         v in req.headers.items()]
    canonical_headers.sort()

    h_names = [k.lower() for k in req.headers]
    h_names.sort()
    signed_headers = ";".join(h_names)

    # Create hash of canonical request
    split_result = urlsplit(req.full_url)  # was self.url
    canonical_form = "{}\n{}\n{}\n{}\n\n{}\n{}".format(
        req.method,
        _encode_path(split_result.path),
        split_result.query,
        "\n".join(canonical_headers),
        signed_headers,
        content_hash)

    canonical_hash = hashlib.sha256(canonical_form.encode("utf-8")).hexdigest()
    # Create and sign "String to sign"
    string_to_sign = "RAI01-ED25519-SHA256\n{}\n{}\n{}".format(
        signature_date, scope, canonical_hash)

    seed = base64.b64decode(ctx.credentials.pkey)
    signing_key = ed25519.SigningKey(seed)
    sig = signing_key.sign(string_to_sign.encode("utf-8")).hex()

    req.headers["authorization"] = "RAI01-ED25519-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}".format(
        ctx.credentials.akey, scope, signed_headers, sig)


# Authenticate the request by signing or adding access token.
def _authenticate(ctx: Context, req: Request) -> Request:
    creds = ctx.credentials
    if creds is None:
        return req
    if type(creds) is ClientCredentials:
        access_token = _get_access_token(ctx, req.full_url)
        req.headers["authorization"] = f"Bearer {access_token}"
        return req
    if type(creds) is AccessKeyCredentials:
        _sign(ctx, req)
        return req
    raise Exception("unknown credential type")


# Issues an RAI REST API request, and returns response contents if successful.
def request(ctx: Context, method: str, url: str, headers={}, data=None, **kwargs):
    headers = _default_headers(url, headers)
    if kwargs:
        url = f"{url}?{_encode_qs(kwargs)}"
    data = _encode(data)
    req = Request(method=method, url=url, headers=headers, data=data)
    req = _authenticate(ctx, req)
    _print_request(req)
    with urlopen(req) as rsp:
        return rsp.read()


def delete(ctx: Context, url: str, data, headers={}, **kwargs) -> str:
    return request(ctx, "DELETE", url, headers=headers, data=data, **kwargs)


def get(ctx: Context, url: str, headers={}, **kwargs) -> str:
    return request(ctx, "GET", url, headers=headers, **kwargs)


def put(ctx: Context, url: str, data, headers={}, **kwargs) -> str:
    return request(ctx, "PUT", url, headers=headers, data=data, **kwargs)


def post(ctx: Context, url: str, data, headers={}, **kwargs) -> str:
    return request(ctx, "POST", url, headers=headers, data=data, **kwargs)


def patch(ctx: Context, url: str, data, headers={}, **kwargs) -> str:
    return request(ctx, "PATCH", url, headers=headers, data=data, **kwargs)
