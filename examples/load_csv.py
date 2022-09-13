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

"""Load a CSV file into the given database with the given relation name."""

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


def run(
    database: str, engine: str, fname: str, relation: str, syntax: dict, profile: str
):
    data = _read(fname)
    relation = relation or _sansext(fname)
    cfg = config.read(profile=profile)
    ctx = api.Context(**cfg)
    rsp = api.load_csv(ctx, database, engine, relation, data, syntax)
    print(json.dumps(rsp, indent=2))


if __name__ == "__main__":
    p = ArgumentParser()
    p.add_argument("database", type=str, help="database name")
    p.add_argument("engine", type=str, help="engine name")
    p.add_argument("file", type=str, help="source file")
    p.add_argument(
        "--header-row",
        type=int,
        default=None,
        help="header row number, 0 for no header (default: 1)",
    )
    p.add_argument("--delim", type=str, default=None, help="field delimiter")
    p.add_argument(
        "--escapechar", type=str, default=None, help="character used to escape quotes"
    )
    p.add_argument("--quotechar", type=str, default=None, help="quoted field character")
    p.add_argument(
        "-r",
        "--relation",
        type=str,
        default=None,
        help="relation name (default: file name)",
    )
    p.add_argument("-p", "--profile", type=str, default="default", help="profile name")
    args = p.parse_args()
    syntax = {}  # find full list of syntax options in the RAI docs
    if args.header_row is not None:
        syntax["header_row"] = args.header_row
    if args.delim:
        syntax["delim"] = args.delim
    if args.escapechar:
        syntax["escapechar"] = args.escapechar
    if args.quotechar:
        syntax["quotechar"] = args.quotechar
    try:
        run(args.database, args.engine, args.file, args.relation, syntax, args.profile)
    except HTTPError as e:
        show.http_error(e)
