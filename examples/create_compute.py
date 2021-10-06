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
from urllib.request import HTTPError
from railib import api
import config


def run(compute: str, size: str):
    cfg = config.read()
    ctx = api.Context(cfg)
    rsp = api.create_compute(ctx, compute, size)
    print(json.dumps(rsp, indent=2))


if __name__ == "__main__":
    p = ArgumentParser()
    p.add_argument("compute", type=str, help="compute name")
    p.add_argument("--size", type=str, default="XS",
                   help="compute size (default: XS)")
    args = p.parse_args()
    try:
        run(args.compute, args.size)
    except HTTPError as e:
        if e.code == 409:
            print(f"Compute '{args.compute}' already exists")
        else: raise
