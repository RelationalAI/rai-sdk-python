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

"""Create a user with an (optional) role."""

from argparse import ArgumentParser
import json
from typing import List
from urllib.request import HTTPError
from railib import api, config, show


def run(user: str, roles: List[api.Role], profile: str):
    cfg = config.read(profile=profile)
    ctx = api.Context(**cfg)
    rsp = api.create_user(ctx, user, roles)
    print(json.dumps(rsp, indent=2))


if __name__ == "__main__":
    p = ArgumentParser()
    p.add_argument("email", type=str, help="user email")
    p.add_argument(
        "--roles",
        action="append",
        default=None,
        help='user roles ("user" (default) or "admin")',
    )
    p.add_argument("-p", "--profile", type=str, help="profile name", default="default")
    args = p.parse_args()
    try:
        roles = [api.Role(r) for r in args.roles] if args.roles else None
        run(args.email, roles, args.profile)
    except HTTPError as e:
        show.http_error(e)
