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


def read(fname: str = "~/.rai/config", profile: str = "default"):
    path = Path(fname).expanduser()
    if not path.is_file():
        raise Exception(f"can't find file: {path}")
    data = _read_config_profile(path, profile)
    akey = data.get("access_key", None)
    if akey is None:
        raise Exception("access_key not found in config")
    fname = data.get("private_key_filename", None)
    if fname is None:
        raise Exception("private_key_filename not found in config")
    pkey = _read_pkey(path.with_name(fname))
    return {
        "akey": akey,
        "pkey": pkey,
        "host": data["host"],
        "port": data.get("port", "443"),
        "region": data.get("region", "us-east"),
        "scheme": data.get("scheme", "https")}
