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
from typing import Union
from urllib.request import HTTPError
import pyarrow as pa

__all__ = [
    "http_error",
    "problems",
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
    if rsp.get("aborted", False):
        print("aborted")
        return
    if rsp.get("output", False):
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
    if rsp.get("status", False):
        print(rsp["status"])


def _show_multipart(parts: list):
    result = []
    content_type_json = b'application/json'
    content_type_arrow_stream = b'application/vnd.apache.arrow.stream'
    for part in parts:
        # split the part
        # part body and headers are separated with CRLFCRLF
        strings = part.split(b'\r\n\r\n')
        # last part is the content/body
        part_value = strings[len(strings) - 1]
        # content is json, decode the part content as json
        if content_type_json in part:
            result.append(json.loads(part_value))
        # if the part has arrow stream, then decode the arrow stream
        # the results are in a form of a tuple/table
        elif content_type_arrow_stream in part:
            with pa.ipc.open_stream(part_value) as reader:
                schema = reader.schema
                batches = [batch for batch in reader]
                table = pa.Table.from_batches(batches=batches, schema=schema)
                result.append(table.to_pydict())

    json.dump(result, sys.stdout, indent=2)


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
def results(rsp: Union[dict, list], format="physical") -> None:
    if rsp is None:
        return
    if format == "wire":
        json.dump(rsp, sys.stdout, indent=2)
    elif format == "physical":
        _show_rel(rsp)
        problems(rsp)
    elif format == "multipart":
        _show_multipart(rsp)
    else:
        raise Exception(f"unknown format: '{format}'")
