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

"""show.results` can be used to print the results of a transaction to the
console."""

from argparse import ArgumentParser
from railib import api, config, show
from show_error import show_error


@show_error
def run(database: str, engine: str, profile: str):
    cfg = config.read(profile=profile)
    ctx = api.Context(**cfg)
    rsp = api.query(ctx, database, engine, "def output = 'a'; 'b'; 'c'")
    show.results(rsp)


if __name__ == "__main__":
    p = ArgumentParser()
    p.add_argument("database", type=str, help="database name")
    p.add_argument("engine", type=str, help="engine name")
    p.add_argument("-p", "--profile", type=str, help="profile name", default="default")
    args = p.parse_args()
    run(args.database, args.engine, args.profile)
