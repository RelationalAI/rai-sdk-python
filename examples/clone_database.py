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
from railib import api
import config


# Clone an existing database by creating a new database and setting the
# optional source argument to the name of the database you want to clone.
def run(database: str, compute: str, source: str):
    cfg = config.read()
    ctx = api.Context(**cfg)
    rsp = api.create_database(
        ctx, database, compute, source=source, overwrite=True)
    print(json.dumps(rsp, indent=2))


if __name__ == "__main__":
    p = ArgumentParser()
    p.add_argument("database", type=str, help="database name")
    p.add_argument("compute", type=str, help="compute name")
    p.add_argument("source", type=str, help="name of database to clone")
    args = p.parse_args()
    run(args.database, args.compute, args.source)
