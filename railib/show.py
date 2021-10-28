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
# limitations under the License.

"""Helpers for formatting errors, problems and query results."""

import json
import sys
from urllib.request import HTTPError


__all__ = [
    "http_error"
    "problems"
    "results"
]


def http_error(e: HTTPError) -> None:
    rsp = json.loads(e.read())
    if rsp:
        print(f"status: {e.status}")
        print(json.dumps(rsp, indent=2))


def _show_row(row: list, end='\n'):
    row = [f'"{item}"' if isinstance(item, str) else str(item) for item in row]
    row = ', '.join(row)
    print(row, end=end)


# Print query response outputs as rel relations.
def _show_rel(rsp: dict) -> None:
    if rsp["aborted"]:
        print("aborted")
        return
    outputs = rsp["output"]
    if len(outputs) == 0:
        print("false")
        return
    count = 0
    for output in outputs:
        cols = output["columns"]
        rkey = output["rel_key"]
        name = rkey["name"]
        keys = rkey["keys"]
        if name == "abort" and len(cols) == 0:
            continue  # ignore constraint results
        if count > 0:
            print()
        sig = '*'.join(keys)
        print(f"// {name} ({sig})")
        rows = list(zip(*cols))
        if len(rows) == 0:
            print("true")
            continue
        for i, row in enumerate(rows):
            if i > 0:
                print(";")
            _show_row(row, end='')
        print()
        count += 1


# Print the problems in the given response dict.
def problems(rsp: dict) -> None:
    if rsp is None:
        return
    problems = rsp.get("problems", None)
    if not problems:
        return
    for problem in problems:
        if problem.get("is_error", False):
            kind = "error"
        elif problem.get("is_exception", False):
            kind = "exception"
        else:
            kind = "warning"  # ?
        print(f"{kind}: {problem['message']}")
        report = problem.get("report", None)
        if report:
            print(report.rstrip())


# Print the results contained in the given response dict.
def results(rsp: dict, format="physical") -> None:
    if rsp is None:
        return
    if format == "wire":
        json.dump(rsp, sys.stdout, indent=2)
    elif format == "physical":
        _show_rel(rsp)
        problems(rsp)
    else:
        raise Exception(f"unknown format: '{format}'")
