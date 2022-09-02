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

"""Create an OAuth client with (optional) permissions."""

import json
from argparse import ArgumentParser
from typing import List
from urllib.request import HTTPError

from railib import api, config, show
from railib.api import Permission


def run(name: str, permissions: List[str], profile: str):
    cfg = config.read(profile=profile)
    ctx = api.Context(**cfg)
    perms = [Permission(p) for p in permissions] if permissions else None
    rsp = api.create_oauth_client(ctx, name, perms)
    print(json.dumps(rsp, indent=2))


if __name__ == "__main__":
    p = ArgumentParser()
    p.add_argument("name", type=str, help="OAuth client name")
    perms = "', '".join(Permission)
    p.add_argument(
        "--permissions",
        nargs="*",
        help="OAuth client permissions. By default it will be "
        "assigned the same permissions as the creating entity "
        "(user or OAuth client). Available: '{0}'".format(perms),
    )
    p.add_argument("-p", "--profile", type=str, help="profile name", default="default")
    args = p.parse_args()
    try:
        run(args.name, args.permissions, args.profile)
    except HTTPError as e:
        show.http_error(e)
