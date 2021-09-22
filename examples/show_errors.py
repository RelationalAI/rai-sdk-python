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


# Many SDK error responses will contain a descriptive error payload.
# The RAI Python SDK is built using the Python urllib3 library, and the
# error payload can be retrieved by reading it from the HTTPError object.
def show_error(e: HTTPError) -> None:
    rsp = json.loads(e.read())
    print(json.dumps(rsp, indent=2))


if __name__ == "__main__":
    try:
        cfg = config.read()
        ctx = api.Context(**cfg)
        rsp = api.create_compute(ctx, "new-compute", "bad-size")  # force 400
    except HTTPError as e:
        show_error(e)
