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

import ed25519
import base64
from datetime import datetime
import hashlib
import json
from pprint import pprint
from urllib import parse
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPError, Request, urlopen

from .__init__ import __version__

__all__ = ["Context", "get", "put", "post", "request"]

from .raiconfig import RAIConfig, ClientCredentials, AccessKeyCredentials

_empty = bytes('', encoding='utf8')

ACCESS_KEY_TOKEN_KEY = "access_token"
CLIENT_ID_KEY = "client_id"
CLIENT_SECRET_KEY = "client_secret"
AUDIENCE_KEY = "audience"
GRANT_TYPE_KEY = "grant_type"
CLIENT_CREDENTIALS_KEY = "client_credentials"
CLIENT_CREDENTIALS_API_URL_PREFIX = "https://login"
CLIENT_CREDENTIALS_API_URL_POSTFIX = ".relationalai.com/oauth/token"
CLIENT_CREDENTIALS_API_SCHEME = "https://"
DEV_ENV_CHAR = "-"


def _print_request(req: Request, level=0):
    if level <= 0:
        return
    if level > 0:
        print(f"{req.method} {req.full_url}")
        if level > 1:
            for k, v in req.headers.items():
                print(f"{k}: {v}")
            if req.data:
                pprint(json.loads(req.data.decode("utf8")))


# Context contains the state required to make rAI REST API calls.
class Context(object):
    def __init__(self, rai_config: RAIConfig):
        self.config = rai_config
        self.service = "transaction"


# Answers if the given list of strings contains a case insensitive match
# for the given term.
def _contains_insensitive(items: list, term: str) -> bool:
    term = term.casefold()
    for item in items:
        item = item.casefold()
        if term == item:
            return True
    return False


# Retrieve the hostname from the given url.
def _gethost(url: str) -> str:
    return urlsplit(url).netloc.split(':')[0]


# Fill in any missing headers.
def _default_headers(url: str, headers: dict) -> None:
    if not _contains_insensitive(headers, "accept"):
        headers["Accept"] = "application/json"
    if not _contains_insensitive(headers, "content-type"):
        headers["Content-Type"] = "application/json"
    if not _contains_insensitive(headers, "host"):
        headers["Host"] = _gethost(url)
    if not _contains_insensitive(headers, "user-agent"):
        headers["User-Agent"] = f"rai-sdk-python/{__version__}"
        # "OpenAPI-Generator/1.0.0/python"


def _encode(data) -> str:
    if not data:
        return data
    if not isinstance(data, str):
        data = json.dumps(data)
    return data.encode("utf8")


# Returns an urlencoded query string.
# Note: the signing algo **requires** query params to be sorted.
def _encode_qs(kwargs: dict) -> str:
    args = [(k, v) for k, v in kwargs.items()]
    args.sort()
    return urlencode(args)


# Implement rAI API key authentication by signing the request and adding the
# required authorization header.
def _sign(ctx: Context, req: Request) -> None:
    ts = datetime.utcnow()

    # ISO8601 date/time strings for time of request
    signature_date = ts.strftime("%Y%m%dT%H%M%SZ")
    scope_date = ts.strftime("%Y%m%d")

    # Authentication scope
    scope = f"{scope_date}/{ctx.config.region}/{ctx.service}/rai01_request"

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
        split_result.path,
        split_result.query,
        "\n".join(canonical_headers),
        signed_headers,
        content_hash)

    canonical_hash = hashlib.sha256(canonical_form.encode("utf-8")).hexdigest()
    # Create and sign "String to sign"
    string_to_sign = "RAI01-ED25519-SHA256\n{}\n{}\n{}".format(
        signature_date,
        scope,
        canonical_hash)

    seed = base64.b64decode(ctx.config.credentials.pkey)
    signing_key = ed25519.SigningKey(seed)
    sig = signing_key.sign(string_to_sign.encode("utf-8")).hex()

    req.headers["authorization"] = "RAI01-ED25519-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}".format(
        ctx.config.credentials.akey, scope, signed_headers, sig)


# request - Makes the RAI api request.
# It will use the provided credentials type to make the request.
# Currently it takes care of AccessKeyCredentials and ClientCredentials.
# AccessKeyCredentials are given precedence over the ClientCredentials, for backward compatibility.
def request(ctx: Context, method: str, url: str, headers={}, data=None, **kwargs):
    _default_headers(url, headers)
    if kwargs:
        url = f"{url}?{_encode_qs(kwargs)}"
    data = _encode(data)
    req = Request(method=method, url=url, headers=headers, data=data)
    if type(ctx.config.credentials) is AccessKeyCredentials:
        _sign(ctx, req)
    elif type(ctx.config.credentials) is ClientCredentials:
        access_token = get_auth_token(ctx.config.credentials, ctx.config.host)
        req.headers["authorization"] = "Bearer " + access_token
    else:
        raise Exception("given type of credentials are not supported")
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


# get_auth_token - Gets the auth token from client credentials api service.
def get_auth_token(client_credentials: ClientCredentials, audience: str):
    # normalize the audience or the host field to include the protocol scheme, like https
    # if the protocol scheme is already there, then it would use the host as-is
    # otherwise it will prepend the scheme, like https://auzre-ux.relationalai.com
    normalized_audience = audience
    if not normalized_audience.startswith(CLIENT_CREDENTIALS_API_SCHEME):
        normalized_audience = CLIENT_CREDENTIALS_API_SCHEME + audience

    # create the payload for api call to get the client credentials (oauth token)
    body = {CLIENT_ID_KEY: client_credentials.client_id,
            CLIENT_SECRET_KEY: client_credentials.client_secret,
            AUDIENCE_KEY: normalized_audience,
            GRANT_TYPE_KEY: CLIENT_CREDENTIALS_KEY}
    data = _encode(body)

    # build the client credentials api url from audidence
    client_credentials_api_url = build_client_credentials_api_url(audience)

    headers = {}
    _default_headers(client_credentials_api_url, headers)

    # make POST call to the API to get an oauth token
    req = Request(method="POST", url=client_credentials_api_url, headers=headers, data=data)
    with urlopen(req) as rsp:
        result = json.loads(rsp.read())
        if result[ACCESS_KEY_TOKEN_KEY]:
            return result[ACCESS_KEY_TOKEN_KEY]

    raise Exception("failed to get the auth token")


# build_auth_token_api_url - Builds the url from audience/host field in the config.
# If the host has an environment like auzre-ux.relationalai.com, then it extracts the environment,
# and then build the url by concatenating CLIENT_CREDENTIALS_API_URL_PREFIX and CLIENT_CREDENTIALS_API_URL_POSTFIX
# like https://login-ux.relationalai.com/oauth/token
# If there is no environment then it would return the url like https://login.relationalai.com/oauth/token
def build_client_credentials_api_url(audience: str):
    environment = None

    dev_env_index = audience.find(DEV_ENV_CHAR)
    if dev_env_index != -1:
        dot_index = audience.find(".", dev_env_index + 1)
        if dot_index != -1:
            environment = audience[dev_env_index + 1:-(len(audience) - dot_index)]
        else:
            environment = audience[dev_env_index + 1]
    if environment:
        return "{}{}{}{}".format(CLIENT_CREDENTIALS_API_URL_PREFIX, DEV_ENV_CHAR, environment,
                                 CLIENT_CREDENTIALS_API_URL_POSTFIX)
    else:
        return "{}{}".format(CLIENT_CREDENTIALS_API_URL_PREFIX, CLIENT_CREDENTIALS_API_URL_POSTFIX)
