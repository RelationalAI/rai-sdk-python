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

"""List EDBs belonging to the given database."""

from argparse import ArgumentParser
import json
from urllib.request import HTTPError
from railib import api, config, show


def run(database: str, engine: str, profile: str):
    cfg = config.read(profile=profile)
    ctx = api.Context(**cfg)
    rsp = api.list_edbs(ctx, database, engine)
    print(json.dumps(rsp, indent=2))


if __name__ == "__main__":
    p = ArgumentParser()
    p.add_argument("database", type=str, help="database name")
    p.add_argument("-p", "--profile", type=str,
                   help="profile name", default="default")
    p.add_argument("engine", type=str, help="engine name")
    args = p.parse_args()
    try:
        run(args.database, args.engine, args.profile)
    except HTTPError as e:
        show.http_error(e)