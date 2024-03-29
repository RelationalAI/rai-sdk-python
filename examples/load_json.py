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
# limitations under the License

"""Load a JSON file into the given database with the given relation name."""

from argparse import ArgumentParser
import json
from os import path
from urllib.request import HTTPError
from railib import api, config, show


def _read(fname: str) -> str:
    with open(fname) as fp:
        return fp.read()


def _sansext(fname: str) -> str:
    return path.splitext(path.basename(fname))[0]


def run(database: str, engine: str, fname: str, relation: str, profile: str):
    data = _read(fname)
    relation = relation or _sansext(fname)
    cfg = config.read(profile=profile)
    ctx = api.Context(**cfg)
    rsp = api.load_json(ctx, database, engine, relation, data)
    print(json.dumps(rsp, indent=2))


if __name__ == "__main__":
    p = ArgumentParser()
    p.add_argument("database", type=str, help="database name")
    p.add_argument("engine", type=str, help="engine name")
    p.add_argument("file", type=str, help="source file")
    p.add_argument(
        "-r",
        "--relation",
        type=str,
        default=None,
        help="relation name (default: file name)",
    )
    p.add_argument("-p", "--profile", type=str, default="default", help="profile name")
    args = p.parse_args()
    try:
        run(args.database, args.engine, args.file, args.relation, args.profile)
    except HTTPError as e:
        show.http_error(e)
