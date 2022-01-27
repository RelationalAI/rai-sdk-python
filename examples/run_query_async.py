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
import json
from argparse import ArgumentParser
from urllib.request import HTTPError
from railib import api, config, show
import pyarrow as pa

def run(database: str, engine: str, command: str, readonly: bool, profile: str):
    cfg = config.read(profile=profile)
    ctx = api.Context(**cfg)
    rsp = api.query_async(ctx, database, engine, command, readonly=readonly)
    result = []
    content_type_json = b'application/json'
    content_type_arrow_stream = b'application/vnd.apache.arrow.stream'
    for part in rsp:
        header_content_type = part.headers[b'content-type']
        if header_content_type == content_type_json:
            result.append(json.loads(bytes(part.content)))
        elif header_content_type == content_type_arrow_stream:
            with pa.ipc.open_stream(part.content) as reader:
                schema = reader.schema
                batches = [batch for batch in reader]
                table = pa.Table.from_batches(batches=batches, schema=schema)
                result.append(table.to_pydict())

    show.results(result, "wire")


if __name__ == "__main__":
    p = ArgumentParser()
    p.add_argument("database", type=str, help="database name")
    p.add_argument("engine", type=str, help="engine name")
    p.add_argument("command", type=str, help="rel source string")
    p.add_argument("--readonly", action="store_true", default=False,
                   help="readonly query (default: false)")
    p.add_argument("-p", "--profile", type=str, default="default",
                   help="profile name")
    args = p.parse_args()
    try:
        run(args.database, args.engine, args.command, args.readonly,
            args.profile)
    except HTTPError as e:
        show.http_error(e)
