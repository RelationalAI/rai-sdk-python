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

"""Reads the RAI SDK config file."""

# Note, the SDK config reader is here for convenience and its use is strictly
# optional. Many clients will chose to use other methods for managing settings
# and client credentials.

import configparser
import json
import os
from pathlib import Path

from .credentials import AccessKeyCredentials, ClientCredentials


def _read_config_profile(fname: str, profile: str) -> dict:
    config = configparser.ConfigParser()
    config.read(fname)
    if profile not in config:
        fname = os.path.basename(fname)
        raise Exception(f"profile '{profile}' not found in {fname}")
    return {k: config[profile][k] for k in config[profile]}


def _read_pkey(fname: Path):
    with open(fname) as fp:
        data = json.load(fp)
        pkey = data.get("sodium", {}).get("seed", None)
        if pkey is None:
            raise Exception("malformed private key")
        return pkey


# Reads access key credentials from the config file. Returns None if they
# do not exist.
def _read_access_key_credentials(data, path: Path):
    akey = data.get("access_key", None)
    if akey is not None:
        fname = data.get("private_key_filename", None)
        if fname is not None:
            pkey = _read_pkey(path.with_name(fname))
            return AccessKeyCredentials(akey, pkey)
    return None


# Read client credentials from the config file. Returns None if they do not
# exist.
def _read_client_credentials(data):
    client_id = data.get("client_id", None)
    if client_id is not None:
        client_secret = data.get("client_secret", None)
        if client_secret is not None:
            client_credentials_url = data.get("client_credentials_url", None)
            return ClientCredentials(client_id, client_secret, client_credentials_url)
    return None


# Reads credentails from config file, preferring client credentials
# if they exist. Returns None if no credentials exist.
def _read_credentials(data, path):
    creds = _read_client_credentials(data)
    if creds is None:
        creds = _read_access_key_credentials(data, path)
    return creds


# Returns settings from config file.
def read(fname: str = "~/.rai/config", profile: str = "default"):
    path = Path(fname).expanduser()
    if not path.is_file():
        raise Exception(f"can't find file: {path}")
    data = _read_config_profile(path, profile)
    creds = _read_credentials(data, path)
    _keys = ["host", "port", "region", "scheme"]
    result = {k: v for k, v in data.items() if k in _keys}
    result["credentials"] = creds
    return result
