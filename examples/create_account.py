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

"""Create an account"""

from argparse import ArgumentParser
import json
from typing import List
from urllib.request import HTTPError
from railib import api, config, show


def run(name: str, idproviders: List[api.IDProvider], adminUsername: str, profile: str):
    cfg = config.read(profile=profile)
    ctx = api.Context(**cfg)
    rsp = api.create_account(ctx, name, adminUsername, idproviders)
    print(json.dumps(rsp, indent=2))


if __name__ == "__main__":
    p = ArgumentParser()
    p.add_argument("name", type=str, help="account name")
    p.add_argument("--adminUsername", type=str, default=None,
                   help='admin username')
    p.add_argument("--idProviders", action='append', default=None,
                   help='identity providers "google-oauth2" and/or "google-apps"')
    p.add_argument("-p", "--profile", type=str,
                   help="profile name", default="default")
    args = p.parse_args()
    try:
        idProviders = [api.IDProvider(r) for r in args.idProviders] if args.idProviders else None
        run(args.name, idProviders, args.adminUsername, args.profile)
    except HTTPError as e:
        print(e)
        show.http_error(e)
