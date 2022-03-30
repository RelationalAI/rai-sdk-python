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

import time
import json
from argparse import ArgumentParser
from urllib.request import HTTPError
from railib import api, config, show

# Answers if the given transaction state is a terminal state.
def is_term_state(state: str) -> bool:
    return state == "COMPLETED" or state == "ABORTED"


def run(database: str, engine: str, command: str, readonly: bool, profile: str):
    cfg = config.read(profile=profile)
    ctx = api.Context(**cfg)
    rsp = api.query_async(ctx, database, engine, command, readonly=readonly)
    if isinstance(rsp, list):  # in case of if short-path, return results directly, no need to poll for state
        show.results(rsp, format = "multipart")
        return

    while True:
        time.sleep(3)
        txnid = rsp["id"]
        rsp = api.get_transaction(ctx, txnid)
        if is_term_state(rsp["state"]):
            rsp = api.get_transaction_metadata(ctx, txnid)
            show.results(rsp, format = "wire")
            rsp = api.get_transaction_results(ctx, txnid)
            show.results(rsp, format = "multipart")
            break


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
