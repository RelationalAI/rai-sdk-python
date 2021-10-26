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


def run(state: str, profile: str):
    cfg = config.read(profile=profile)
    ctx = api.Context(**cfg)
    rsp = api.list_computes(ctx, state=state)
    print(f"There are {len(rsp)} items in the response")
    print(json.dumps(rsp, indent=2))


if __name__ == "__main__":
    p = ArgumentParser()
    state_default = "PROVISIONED"
    p.add_argument("--state",
                   type=str,
                   default=state_default,
                   help=f"state filter (default: {state_default})" + " ALL | DELETED | PROVISIONED | ..."
                   )
    p.add_argument("-p", "--profile", type=str, help="profile name", default="default")
    args = p.parse_args()
    if args.state == "ALL":
        args.state = None
    run(args.state, args.profile)
