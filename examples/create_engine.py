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
from railib import api, config, show
from railib.api import EngineSize


def run(engine: str, size: str):
    cfg = config.read()
    ctx = api.Context(**cfg)
    rsp = api.create_engine(ctx, engine, EngineSize(size))
    print(json.dumps(rsp, indent=2))


if __name__ == "__main__":
    p = ArgumentParser()
    p.add_argument("engine", type=str, help="engine name")
    p.add_argument("--size", type=str, default="XS",
                   help="compute size (default: XS)")
    p.add_argument("-p", "--profile", type=str, help="profile name", default="default")
    args = p.parse_args()
    try:
        run(args.engine, args.size, args.profile)
    except HTTPError as e:
        show.http_error(e)
