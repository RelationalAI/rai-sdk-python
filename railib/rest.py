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
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, urlopen

from . import utils

__all__ = ["Context", "get", "put", "post", "request"]

from .client_credentials_service import ClientCredentialsService

from .rai_config import RAIConfig, ClientCredentials, AccessKeyCredentials

_empty = bytes('', encoding='utf8')


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
        self.client_credentials_service = ClientCredentialsService(self.config)


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
    utils.default_headers(url, headers)
    if kwargs:
        url = f"{url}?{_encode_qs(kwargs)}"
    data = utils.encode(data)
    req = Request(method=method, url=url, headers=headers, data=data)
    if type(ctx.config.credentials) is AccessKeyCredentials:
        _sign(ctx, req)
    elif type(ctx.config.credentials) is ClientCredentials:
        access_token = ctx.client_credentials_service.get_access_token()
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

