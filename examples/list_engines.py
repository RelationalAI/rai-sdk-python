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

"""List all engines."""

from argparse import ArgumentParser
import json
from railib import api, config
from show_error import show_error


@show_error
def run(state: str, profile: str):
    cfg = config.read(profile=profile)
    ctx = api.Context(**cfg)
    rsp = api.list_engines(ctx, state=state)
    print(f"There are {len(rsp)} items in the response")
    print(json.dumps(rsp, indent=2))


if __name__ == "__main__":
    p = ArgumentParser()
    p.add_argument(
        "--state", type=str, default=None, help="state filter (default: none"
    )
    p.add_argument("-p", "--profile", type=str, help="profile name", default="default")
    args = p.parse_args()
    run(args.state, args.profile)
