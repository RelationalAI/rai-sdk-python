# Copyright 2021-2022 RelationalAI, Inc.
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

"""Fetch results for the given transaction."""

from argparse import ArgumentParser
from urllib.request import HTTPError
from railib import api, config, show


def run(id: str, profile: str):
    cfg = config.read(profile=profile)
    ctx = api.Context(**cfg)
    rsp = api.get_transaction_results_and_problems(ctx, id)
    show.results(rsp)


if __name__ == "__main__":
    p = ArgumentParser()
    p.add_argument("-p", "--profile", type=str, help="profile name", default="default")
    p.add_argument("id", type=str, help="transaction id")
    args = p.parse_args()
    try:
        run(args.id, args.profile)
    except HTTPError as e:
        show.http_error(e)
