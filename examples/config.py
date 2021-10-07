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

"""Helper class for loading the RAI SDK config file."""

import configparser
import json
import os
from pathlib import Path

from railib.raiconfig import AccessKeyCredentials, ClientCredentials, RAIConfig


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


# read - Reads the RAI config along with the credentials.
# It would first try to read the AccessKeyCredentials, if not found, then it would try reading -
# the ClientCredentials. AccessKeyCredentials are preferred credentials if both the credentials -
# are present in the config file. AccessKeyCredentials are supported for backward compatibility.
def read(fname: str = "~/.rai/config", profile: str = "default"):
    path = Path(fname).expanduser()
    if not path.is_file():
        raise Exception(f"can't find file: {path}")
    data = _read_config_profile(path, profile)

    # Try reading all the credentials
    credentials = [read_access_key_credentials(data, path), read_client_credentials(data)]
    if len(credentials) == 0:
        Exception("no credentials found in the config")
    elif len(credentials) > 1:
        Exception("multiple credentials found in the config")

    # Return the RAIConfig along with the credentials
    return RAIConfig(
                data.get("host", "localhost"),
                data.get("port", "443"),
                data.get("region", "us-east"),
                data.get("scheme", "https"), credentials[0])


# read_access_key_credentials - Tries to read access key credentials from the config file.
# Will raise exception if partial credentials are found, for example, access_key is found -
# but private_key is not present. It will return None if access_key field is not present.
def read_access_key_credentials(data, path: Path):
    akey = data.get("access_key", None)
    if akey is not None:
        pk_fname = data.get("private_key_filename", None)
        if pk_fname is not None:
            pkey = _read_pkey(path.with_name(pk_fname))
            return AccessKeyCredentials(akey, pkey)
        else:
            raise Exception("private_key_filename not found in the config")

    return None


# read_client_credentials - Tries to read client credentials from the config file.
# Will raise exception if partial credentials are found, for example, client_id is found -
# but client_secret is not found. It will return None if client_id field is not present.
def read_client_credentials(data):
    client_id = data.get("client_id", None)
    if client_id is not None:
        client_secret = data.get("client_secret", None)
        if client_secret is not None:
            return ClientCredentials(client_id, client_secret)
        else:
            raise Exception("client_secret not found in the config")

    return None



