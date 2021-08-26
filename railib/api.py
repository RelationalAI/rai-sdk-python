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

"""Operation level interface to the RelationalAI REST API."""

import json

from . import rest

PATH_COMPUTE = "/compute"
PATH_DATABASE = "/database"
PATH_TRANSACTION = "/transaction"
PATH_USER = "/user"

# Compute sizes
COMPUTE_XS = "XS"
COMPUTE_S = "S"
COMPUTE_M = "M"
COMPUTE_L = "L"
COMPUTE_XL = "XL"

# Database modes
MODE_OPEN = "OPEN"
MODE_CREATE = "CREATE"
MODE_CREATE_OVERWRITE = "CREATE_OVERWRITE"
MODE_OPEN_OR_CREATE = "OPEN_OR_CREATE"
MODE_CLONE = "CLONE"
MODE_CLONE_OVERWRITE = "CLONE_OVERWRITE"


# Context contains the state required to make rAI API calls.
class Context(rest.Context):
    def __init__(self, region=None, scheme=None, host=None, port=None, akey=None, pkey=None):
        super().__init__(region=region, akey=akey, pkey=pkey)
        self.scheme = scheme
        self.host = host
        self.port = port


# Cosntruct a URL from the given context and path.
def _mkurl(ctx: Context, path: str) -> str:
    return f"{ctx.scheme}://{ctx.host}:{ctx.port}{path}"


# Retrieve an individual resource.
def _get_resource(ctx: Context, path: str, key=None, **kwargs) -> dict:
    url = _mkurl(ctx, path)
    rsp = rest.get(ctx, url, **kwargs)
    rsp = json.loads(rsp)
    if key:
        rsp = rsp[key]
    if rsp and isinstance(rsp, list):
        assert len(rsp) == 1
        return rsp[0]
    return rsp


# Retrieve a generic collection of resources.
def _list_collection(ctx, path: str, key=None, **kwargs):
    rsp = rest.get(ctx, _mkurl(ctx, path), **kwargs)
    rsp = json.loads(rsp)
    return rsp[key] if key else rsp


def create_compute(ctx: Context, compute: str, size: str):
    data = {
        "region": ctx.region,
        "name": compute,
        "size": str(size)}
    url = _mkurl(ctx, PATH_COMPUTE)
    rsp = rest.put(ctx, url, data)
    return json.loads(rsp)


def create_user(ctx: Context, user: str):
    raise Exception("not implemented")


# Derives the database open_mode based on the given arguments.
def _create_mode(source_name: str, overwrite: bool) -> str:
    if source_name is not None:
        result = MODE_CLONE_OVERWRITE if overwrite else MODE_CLONE
    else:
        result = MODE_CREATE_OVERWRITE if overwrite else MODE_CREATE
    return str(result)


def delete_compute(ctx: Context, compute: str) -> dict:
    data = {"name": compute}
    url = _mkurl(ctx, PATH_COMPUTE)
    rsp = rest.delete(ctx, url, data)
    return json.loads(rsp)


def delete_database(ctx: Context, database: str) -> dict:
    raise Exception("not implemented")


def delete_user(ctx: Context, user: str) -> dict:
    raise Exception("not implemented")


def get_compute(ctx: Context, compute: str) -> dict:
    return _get_resource(ctx, PATH_COMPUTE, name=compute, key="computes")


def get_database(ctx: Context, database: str) -> dict:
    return _get_resource(ctx, PATH_DATABASE, name=database, key="databases")


def get_user(ctx: Context, user: str) -> dict:
    return _get_resource(ctx, PATH_USER, name=user, key="users")


def list_computes(ctx: Context, state=None) -> list:
    kwargs = {}
    if state is not None:
        kwargs["state"] = state
    return _list_collection(ctx, PATH_COMPUTE, key="computes", **kwargs)


def list_databases(ctx: Context) -> list:
    return _list_collection(ctx, PATH_DATABASE, key="databases")


def list_users(ctx: Context) -> list:
    return _list_collection(ctx, PATH_USER, key="users")


#
# Transaction endpoint
#

class Transaction(object):
    def __init__(self, database: str, compute: str, abort=False,
                 mode: str = MODE_OPEN, nowait_durable=False, readonly=False,
                 source_database=None):
        self.abort = abort
        self.database = database
        self.compute = compute
        self.mode = mode
        self.nowait_durable = nowait_durable
        self.readonly = readonly
        self.source_database = source_database

    # Wrap each of the given actions in a LabeledAction structure.
    def _actions(self, *args):
        actions = []
        for i, action in enumerate(*args):
            actions.append({
                "name": f"action{i}",
                "type": "LabeledAction",
                "action": action})
        return actions

    @property
    def data(self):
        # can't use __dict__ because we need to control which attrs appears
        result = {
            "abort": self.abort,
            "dbname": self.database,
            "mode": self.mode,
            "nowait_durable": self.nowait_durable,
            "readonly": self.readonly,
            "type": "Transaction",
            "version": 0  # 25
        }
        if self.compute is not None:
            result["computeName"] = self.compute
        if self.source_database is not None:
            result["source_dbname"] = self.source_database
        return result

    def run(self, ctx: Context, *args) -> dict:
        data = self.data
        data["actions"] = self._actions(args)
        # several of the request params are duplicated in the query
        kwargs = {
            "dbname": self.database,
            "compute_name": self.compute,
            "open_mode": self.mode,
            "region": ctx.region}
        url = _mkurl(ctx, PATH_TRANSACTION)
        rsp = rest.post(ctx, url, data, **kwargs)
        return json.loads(rsp)


def _install_action(name: str, source: str) -> dict:
    return {"type": "InstallAction", "sources": [_source(name, source)]}


def _list_action():
    return {"type": "ListSourceAction"}


def _query_action(source: str, name="query", inputs: list = None,
                  outputs: list = None) -> dict:
    return {
        "type": "QueryAction",
        "source": _source("query", source),
        "persist": [],
        "inputs": inputs or [],    # required?
        "outputs": outputs or []}  # required?


def _source(name: str, source: str) -> dict:
    return {
        "type": "Source",
        "name": name,
        "path": "",       # todo: check if required?
        "value": source}


# Returns full list of source objects, including source values.
def _list_sources(ctx: Context, database: str, compute: str) -> dict:
    tx = Transaction(database, compute, mode=MODE_OPEN)
    rsp = tx.run(ctx, _list_action())
    actions = rsp["actions"]
    assert len(actions) == 1
    action = actions[0]
    sources = action["result"]["sources"]
    return sources


def create_database(ctx: Context, database: str, compute: str,
                    source: str = None, overwrite=False) -> dict:
    overwrite = True
    mode = _create_mode(source, overwrite)
    tx = Transaction(database, compute, mode=mode, source_database=source)
    return tx.run(ctx)


# Returns the source value for the named source.
def get_source(ctx: Context, database: str, compute: str, name: str) -> str:
    sources = _list_sources(ctx, database, compute)
    for source in sources:
        if source["name"] == name:
            return source["value"]
    raise Exception(f"source '{name}' not found")


def install_source(ctx: Context, database: str, compute: str, sources: dict) -> dict:
    tx = Transaction(database, compute, mode=MODE_OPEN, readonly=False)
    actions = [_install_action(name, source)
               for name, source in sources.items()]
    return tx.run(ctx, *actions)


# Returns a list of source names installed in the given database.
def list_sources(ctx: Context, database: str, compute: str) -> list:
    sources = _list_sources(ctx, database, compute)
    return [source["name"] for source in sources]


def query(ctx: Context, database: str, compute: str, command: str, **kwargs) -> dict:
    tx = Transaction(database, compute, readonly=True)
    return tx.run(ctx, _query_action(command))
