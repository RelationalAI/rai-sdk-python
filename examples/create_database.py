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

from argparse import ArgumentParser
import json
from railib import api, config
from urllib.request import HTTPError

def show_error(e: HTTPError) -> None:
    r = e.read()
    if len(r) > 0:
        rsp = json.loads(r)
        print(f"Got error, status: {e.status}")
        print(json.dumps(rsp, indent=2))
    else:
        print("There is no descriptive error message, got:", e)


def run(database: str, compute: str, overwrite: bool, profile: str):
    cfg = config.read(profile=profile)
    ctx = api.Context(**cfg)
    try:
        rsp = api.create_database(ctx, database, compute, overwrite=overwrite)
        print(json.dumps(rsp, indent=2))
    except HTTPError as e:
        show_error(e)


if __name__ == "__main__":
    p = ArgumentParser()
    p.add_argument("database", type=str, help="database name")
    p.add_argument("compute", type=str, help="compute name")
    p.add_argument("--overwrite", action="store_true",
                   help="overwrite existing database")
    p.add_argument("-p", "--profile", type=str, help="profile name", default="default")
    args = p.parse_args()
    run(args.database, args.compute, args.overwrite, args.profile)
