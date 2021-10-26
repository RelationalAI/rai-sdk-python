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

from enum import Enum, unique
import json

from . import rest

PATH_ENGINE = "/compute"
PATH_DATABASE = "/database"
PATH_TRANSACTION = "/transaction"
PATH_USER = "/users"


# Engine sizes
@unique
class EngineSize(str, Enum):
    XS = "XS"
    S = "S"
    M = "M"
    L = "L"
    XL = "XL"


# Database modes
@unique
class Mode(str, Enum):
    OPEN = "OPEN"
    CREATE = "CREATE"
    CREATE_OVERWRITE = "CREATE_OVERWRITE"
    OPEN_OR_CREATE = "OPEN_OR_CREATE"
    CLONE = "CLONE"
    CLONE_OVERWRITE = "CLONE_OVERWRITE"


__all__ = [
    "Context",
    "EngineSize",
    "Mode",
    "create_database",
    "create_engine",
    "create_user",
    "delete_database",
    "delete_engine",
    "delete_source",
    "delete_user",
    "get_database",
    "get_engine",
    "get_source",
    "get_user",
    "list_databases",
    "list_edb",
    "list_engines",
    "list_sources",
    "list_users",
    "query",
]


# Context contains the state required to make rAI API calls.
class Context(rest.Context):
    def __init__(self, host: str = None, port: str = None, scheme: str = None,
                 region: str = None, credentials=None):
        super().__init__(region=region, credentials=credentials)
        self.host = host
        self.port = port or "443"
        self.scheme = scheme or "https"


# Construct a URL from the given context and path.
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


def create_engine(ctx: Context, engine: str, size: EngineSize = EngineSize.XS):
    data = {
        "region": ctx.region,
        "name": engine,
        "size": size.value}
    url = _mkurl(ctx, PATH_ENGINE)
    rsp = rest.put(ctx, url, data)
    return json.loads(rsp)


def create_user(ctx: Context, user: str):
    raise Exception("not implemented")


# Derives the database open_mode based on the given arguments.
def _create_mode(source_name: str, overwrite: bool) -> Mode:
    if source_name is not None:
        result = Mode.CLONE_OVERWRITE if overwrite else Mode.CLONE
    else:
        result = Mode.CREATE_OVERWRITE if overwrite else Mode.CREATE
    return result


def delete_engine(ctx: Context, engine: str) -> dict:
    data = {"name": engine}
    url = _mkurl(ctx, PATH_ENGINE)
    rsp = rest.delete(ctx, url, data)
    return json.loads(rsp)


def delete_database(ctx: Context, database: str) -> dict:
    raise Exception("not implemented")


def delete_user(ctx: Context, user: str) -> dict:
    raise Exception("not implemented")


def get_engine(ctx: Context, engine: str) -> dict:
    return _get_resource(ctx, PATH_ENGINE, name=engine, key="computes")


def get_database(ctx: Context, database: str) -> dict:
    return _get_resource(ctx, PATH_DATABASE, name=database, key="databases")


def get_user(ctx: Context, user: str) -> dict:
    return _get_resource(ctx, f"{PATH_USER}/{user}", name=user)
    # return _get_resource(ctx, PATH_USER, name=user, key="users")


def list_engines(ctx: Context, state=None) -> list:
    kwargs = {}
    if state is not None:
        kwargs["state"] = state
    return _list_collection(ctx, PATH_ENGINE, key="computes", **kwargs)


def list_databases(ctx: Context, state=None) -> list:
    kwargs = {}
    if state is not None:
        kwargs["state"] = state
    return _list_collection(ctx, PATH_DATABASE, key="databases", **kwargs)


def list_users(ctx: Context) -> list:
    return _list_collection(ctx, PATH_USER, key="users")


#
# Transaction endpoint
#

class Transaction(object):
    def __init__(self, database: str, engine: str, abort=False,
                 mode: Mode = Mode.OPEN, nowait_durable=False, readonly=False,
                 source_database=None):
        self.abort = abort
        self.database = database
        self.engine = engine
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
        result = {
            "abort": self.abort,
            "dbname": self.database,
            "mode": self.mode.value,
            "nowait_durable": self.nowait_durable,
            "readonly": self.readonly,
            "type": "Transaction",
            "version": 0  # 25
        }
        if self.engine is not None:
            result["computeName"] = self.engine
        if self.source_database is not None:
            result["source_dbname"] = self.source_database
        return result

    def run(self, ctx: Context, *args) -> dict:
        data = self.data
        data["actions"] = self._actions(args)
        # several of the request params are duplicated in the query
        kwargs = {
            "dbname": self.database,
            "compute_name": self.engine,
            "open_mode": self.mode.value,
            "region": ctx.region}
        if self.source_database:
            kwargs["source_dbname"] = self.source_database
        url = _mkurl(ctx, PATH_TRANSACTION)
        rsp = rest.post(ctx, url, data, **kwargs)
        return json.loads(rsp)


def _delete_source_action(name: str) -> dict:
    return {"type": "ModifyWorkspaceAction", "delete_source": [name]}


def _install_source_action(name: str, source: str) -> dict:
    return {"type": "InstallAction", "sources": [_source(name, source)]}


def _list_action():
    return {"type": "ListSourceAction"}


def _list_edb_action():
    return {"type": "ListEdbAction"}


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
def _list_sources(ctx: Context, database: str, engine: str) -> dict:
    tx = Transaction(database, engine, mode=Mode.OPEN)
    rsp = tx.run(ctx, _list_action())
    actions = rsp["actions"]
    assert len(actions) == 1
    action = actions[0]
    sources = action["result"]["sources"]
    return sources


def create_database(ctx: Context, database: str, engine: str,
                    source: str = None, overwrite=False) -> dict:
    mode = _create_mode(source, overwrite)
    tx = Transaction(database, engine, mode=mode, source_database=source)
    return tx.run(ctx)


def delete_source(ctx: Context, database: str, engine: str, source: str) -> dict:
    tx = Transaction(database, engine, mode=Mode.OPEN, readonly=False)
    actions = [_delete_source_action(source)]
    return tx.run(ctx, *actions)


# Returns the source value for the named source.
def get_source(ctx: Context, database: str, engine: str, name: str) -> str:
    sources = _list_sources(ctx, database, engine)
    for source in sources:
        if source["name"] == name:
            return source["value"]
    raise Exception(f"source '{name}' not found")


def install_source(ctx: Context, database: str, engine: str, sources: dict) -> dict:
    tx = Transaction(database, engine, mode=Mode.OPEN, readonly=False)
    actions = [_install_source_action(name, source)
               for name, source in sources.items()]
    return tx.run(ctx, *actions)


def list_edb(ctx: Context, database: str, engine: str) -> list:
    tx = Transaction(database, engine, mode=Mode.OPEN)
    rsp = tx.run(ctx, _list_edb_action())
    actions = rsp["actions"]
    assert len(actions) == 1
    action = actions[0]
    rels = action["result"]["rels"]
    return rels


# Returns a list of source names installed in the given database.
def list_sources(ctx: Context, database: str, engine: str) -> list:
    sources = _list_sources(ctx, database, engine)
    return [source["name"] for source in sources]


def query(ctx: Context, database: str, engine: str, command: str, readonly: bool = True) -> dict:
    tx = Transaction(database, engine, readonly=readonly)
    return tx.run(ctx, _query_action(command))


create_compute = create_engine  # deprecated, use create_engine
delete_compute = delete_engine  # deprecated, use delete_engine
get_compute = get_engine        # deprecated, use get_engine
list_computes = list_engines    # deprecated, use list_engines
