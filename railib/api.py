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

import io
import json
import pyarrow as pa
import time
from enum import Enum, unique
from typing import List, Union
from . import rest

PATH_ENGINE = "/compute"
PATH_DATABASE = "/database"
PATH_TRANSACTION = "/transaction"
PATH_TRANSACTIONS = "/transactions"
PATH_USER = "/users"
PATH_OAUTH_CLIENT = "/oauth-clients"


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


# User roles
@unique
class Role(str, Enum):
    USER = "user"
    ADMIN = "admin"


# User/OAuth-client permissions
@unique
class Permission(str, Enum):
    # computes
    CREATE_COMPUTE = "create:compute"
    DELETE_COMPUTE = "delete:compute"
    LIST_COMPUTES = "list:compute"
    READ_COMPUTE = "read:compute"
    # databases
    LIST_DATABASES = "list:database"
    UPDATE_DATABASE = "update:database"
    DELETE_DATABASE = "delete:database"
    # transactions
    RUN_TRANSACTION = "run:transaction"
    READ_TRANSACTION = "read:transaction"
    # credits
    READ_CREDITS_USAGE = "read:credits_usage"
    # oauth clients
    CREATE_OAUTH_CLIENT = "create:oauth_client"
    READ_OAUTH_CLIENT = "read:oauth_client"
    LIST_OAUTH_CLIENTS = "list:oauth_client"
    UPDATE_OAUTH_CLIENT = "update:oauth_client"
    DELETE_OAUTH_CLIENT = "delete:oauth_client"
    ROTATE_OAUTH_CLIENT_SECRET = "rotate:oauth_client_secret"
    # users
    CREATE_USER = "create:user"
    LIST_USERS = "list:user"
    READ_USER = "read:user"
    UPDATE_USER = "update:user"
    # roles
    LIST_ROLES = "list:role"
    READ_ROLE = "read:role"
    # permissions
    LIST_PERMISSIONS = "list:permission"
    # access keys
    CREATE_ACCESS_KEY = "create:accesskey"
    LIST_ACCESS_KEYS = "list:accesskey"


__all__ = [
    "Context",
    "EngineSize",
    "Mode",
    "Role",
    "Permission",
    "create_database",
    "create_engine",
    "create_user",
    "create_oauth_client",
    "delete_database",
    "delete_engine",
    "delete_model",
    "disable_user",
    "enable_user",
    "delete_oauth_client",
    "get_database",
    "get_engine",
    "get_model",
    "get_oauth_client",
    "get_transaction",
    "get_transaction_metadata",
    "list_transactions",
    "get_transaction_results_and_problems",
    "cancel_transaction",
    "get_user",
    "list_databases",
    "list_edbs",
    "list_engines",
    "list_models",
    "list_users",
    "list_oauth_clients",
    "load_csv",
    "update_user",
    "query",
    "query_async",
]


# Context contains the state required to make rAI API calls.
class Context(rest.Context):
    def __init__(self, host: str = None, port: str = None, scheme: str = None,
                 region: str = None, credentials=None, audience: str = None):
        super().__init__(region=region, credentials=credentials)
        self.host = host
        self.port = port or "443"
        self.scheme = scheme or "https"
        self.audience = audience


# Construct a URL from the given context and path.
def _mkurl(ctx: Context, path: str) -> str:
    return f"{ctx.scheme}://{ctx.host}:{ctx.port}{path}"


# Retrieve an individual resource.
def _get_resource(ctx: Context, path: str, key=None, **kwargs) -> dict:
    url = _mkurl(ctx, path)
    rsp = rest.get(ctx, url, **kwargs)
    rsp = json.loads(rsp.read())
    if key:
        rsp = rsp[key]
    if rsp and isinstance(rsp, list):
        assert len(rsp) == 1
        return rsp[0]
    return rsp


# Retrieve a generic collection of resources.
def _get_collection(ctx, path: str, key=None, **kwargs):
    rsp = rest.get(ctx, _mkurl(ctx, path), **kwargs)
    rsp = json.loads(rsp.read())
    return rsp[key] if key else rsp


# Parses "multipart/form-data" responses. It returns the parts in a list.
def _parse_multipart(content_type: str, content: bytes) -> list:
    result = []
    content_type_json = b'application/json'
    content_type_arrow = b'application/vnd.apache.arrow.stream'
    boundary = _extract_multipart_boundary(content_type)

    parts = content.split(b'\r\n' + boundary)
    for i, part in enumerate(parts):
        # fix the first part, it may contain the boundary
        if i == 0:
            part = part.split(boundary)[-1]
        # Skip "--\r\n"
        if b'--\r\n' in part:
            continue
        # Part body and headers are separated with CRLFCRLF. Get the body.
        part_value = part.split(b'\r\n\r\n')[-1]
        if content_type_json in part:
            result.append(json.loads(part_value))
        # if the part has arrow stream, then decode the arrow stream
        # the results are in a form of a tuple/table
        elif content_type_arrow in part:
            with pa.ipc.open_stream(part_value) as reader:
                schema = reader.schema
                batches = [batch for batch in reader]
                table = pa.Table.from_batches(batches=batches, schema=schema)
                result.append(table.to_pydict())

    return result


# Finds and returns the multipart form-data boundary.
# Example:
#   "Content-Type: multipart/form-data; boundary=d6d47cadd395db7f6648a2ffb65751eb"
def _extract_multipart_boundary(content_type: str) -> bytes:
    if "boundary=" not in content_type:
        raise Exception("no multipart boundary")
    return b'--' + content_type.split("boundary=")[-1].encode("utf-8")


def create_engine(ctx: Context, engine: str, size: EngineSize = EngineSize.XS):
    data = {
        "region": ctx.region,
        "name": engine,
        "size": size.value}
    url = _mkurl(ctx, PATH_ENGINE)
    rsp = rest.put(ctx, url, data)
    return json.loads(rsp.read())


def create_user(ctx: Context, email: str, roles: List[Role] = None):
    rs = roles or []
    data = {
        "email": email,
        "roles": [r.value for r in rs]}
    url = _mkurl(ctx, PATH_USER)
    rsp = rest.post(ctx, url, data)
    return json.loads(rsp.read())


def create_oauth_client(ctx: Context, name: str, permissions: List[Permission] = None):
    ps = permissions or []
    data = {
        "name": name,
        "permissions": ps}
    url = _mkurl(ctx, PATH_OAUTH_CLIENT)
    rsp = rest.post(ctx, url, data)
    return json.loads(rsp.read())["client"]


# Derives the database open_mode based on the given arguments.
def _create_mode(source_database: str, overwrite: bool) -> Mode:
    if source_database is not None:
        result = Mode.CLONE_OVERWRITE if overwrite else Mode.CLONE
    else:
        result = Mode.CREATE_OVERWRITE if overwrite else Mode.CREATE
    return result


def delete_database(ctx: Context, database: str) -> dict:
    data = {"name": database}
    url = _mkurl(ctx, PATH_DATABASE)
    rsp = rest.delete(ctx, url, data)
    return json.loads(rsp.read())


def delete_engine(ctx: Context, engine: str) -> dict:
    data = {"name": engine}
    url = _mkurl(ctx, PATH_ENGINE)
    rsp = rest.delete(ctx, url, data)
    return json.loads(rsp.read())


def delete_user(ctx: Context, id: str) -> dict:
    url = _mkurl(ctx, f"{PATH_USER}/{id}")
    rsp = rest.delete(ctx, url, None)
    return json.loads(rsp.read())


def disable_user(ctx: Context, userid: str) -> dict:
    return update_user(ctx, userid, status="INACTIVE")


def delete_oauth_client(ctx: Context, id: str) -> dict:
    url = _mkurl(ctx, f"{PATH_OAUTH_CLIENT}/{id}")
    rsp = rest.delete(ctx, url, None)
    return json.loads(rsp.read())


def enable_user(ctx: Context, userid: str) -> dict:
    return update_user(ctx, userid, status="ACTIVE")


def get_engine(ctx: Context, engine: str) -> dict:
    return _get_resource(ctx, PATH_ENGINE, name=engine, deleted_on="", key="computes")


def get_database(ctx: Context, database: str) -> dict:
    return _get_resource(ctx, PATH_DATABASE, name=database, key="databases")


def get_oauth_client(ctx: Context, id: str) -> dict:
    return _get_resource(ctx, f"{PATH_OAUTH_CLIENT}/{id}", key="client")


def get_transaction(ctx: Context, id: str) -> dict:
    return _get_resource(ctx, f"{PATH_TRANSACTIONS}/{id}", key="transaction")


def get_transaction_metadata(ctx: Context, id: str) -> list:
    return _get_collection(ctx, f"{PATH_TRANSACTIONS}/{id}/metadata")


def list_transactions(ctx: Context) -> list:
    return _get_collection(ctx, PATH_TRANSACTIONS, key="transactions")


def get_transaction_problems(ctx: Context, id: str) -> list:
    return _get_collection(ctx, f"{PATH_TRANSACTIONS}/{id}/problems")


def get_transaction_results(ctx: Context, id: str) -> list:
    url = _mkurl(ctx, f"{PATH_TRANSACTIONS}/{id}/results")
    rsp = rest.get(ctx, url)
    content_type = rsp.headers.get('content-type', "")
    if "multipart/form-data" not in content_type:
        raise Exception("invalid response type")
    return _parse_multipart(content_type, rsp.read())

# When problems are part of the results relations, this function should be deprecated, get_transaction_results should be called instead
def get_transaction_results_and_problems(ctx: Context, id: str) -> list:
    rsp = []
    rsp.append(get_transaction_problems(ctx, id))
    rsp.append(get_transaction_results(ctx, id))
    return rsp

def cancel_transaction(ctx: Context, id: str) -> dict:
    rsp = rest.post(ctx, _mkurl(ctx, f"{PATH_TRANSACTIONS}/{id}/cancel"), {})
    return json.loads(rsp.read())


def get_user(ctx: Context, userid: str) -> dict:
    return _get_resource(ctx, f"{PATH_USER}/{userid}", name=userid)


def list_engines(ctx: Context, state=None) -> list:
    kwargs = {}
    if state is not None:
        kwargs["state"] = state
    return _get_collection(ctx, PATH_ENGINE, key="computes", **kwargs)


def list_databases(ctx: Context, state=None) -> list:
    kwargs = {}
    if state is not None:
        kwargs["state"] = state
    return _get_collection(ctx, PATH_DATABASE, key="databases", **kwargs)


def list_users(ctx: Context) -> list:
    return _get_collection(ctx, PATH_USER, key="users")


def list_oauth_clients(ctx: Context) -> list:
    return _get_collection(ctx, PATH_OAUTH_CLIENT, key="clients")


def update_user(ctx: Context, userid: str, status: str = None, roles=None):
    data = {}
    if status is not None:
        data["status"] = status
    if roles is not None:
        data["roles"] = roles
    url = _mkurl(ctx, f"{PATH_USER}/{userid}")
    rsp = rest.patch(ctx, url, data)
    return json.loads(rsp.read())


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
            "version": 0
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
        return json.loads(rsp.read())


class TransactionAsync(object):
    def __init__(self, database: str, engine: str, nowait_durable=False, readonly=False):
        self.database = database
        self.engine = engine
        self.nowait_durable = nowait_durable
        self.readonly = readonly

    @property
    def data(self):
        result = {
            "dbname": self.database,
            "nowait_durable": self.nowait_durable,
            "readonly": self.readonly,
            # "sync_mode": "async"
        }
        if self.engine is not None:
            result["engine_name"] = self.engine
        return result

    def run(self, ctx: Context, command: str, inputs: dict = None) -> Union[dict, list]:
        data = self.data
        data["query"] = command
        if not inputs is None:
            inputs = [_query_action_input(k, v) for k, v in inputs.items()]
            data["v1_inputs"] = inputs
        rsp = rest.post(ctx, _mkurl(ctx, PATH_TRANSACTIONS), data)
        content_type = rsp.headers.get('content-type', None)
        content = rsp.read()
        # todo: response model should be based on status code (200 v. 201)
        # async mode
        if content_type.lower() == "application/json":
            return json.loads(content)
        # sync mode
        if "multipart/form-data" in content_type.lower():
            return _parse_multipart(content_type, content)
        raise Exception("invalid response type")


def _delete_model_action(name: str) -> dict:
    return {"type": "ModifyWorkspaceAction", "delete_source": [name]}


def _install_model_action(name: str, model: str) -> dict:
    return {"type": "InstallAction", "sources": [_model(name, model)]}


def _list_action():
    return {"type": "ListSourceAction"}


def _list_edb_action():
    return {"type": "ListEdbAction"}


# Return rel key correponding to the given name and list of keys.
def _rel_key(name: str, keys: list) -> dict:
    return {
        "type": "RelKey",
        "name": name,
        "keys": keys,
        "values": []}


# Return the rel typename corresponding to the type of the given value.
def _rel_typename(v):
    if isinstance(v, str):
        return "RAI_VariableSizeStrings.VariableSizeString"
    raise TypeError("unsupported input type: {v.__class__.__name__")  # todo


# Return a qeury action input corresponding to the given name, value pair.
def _query_action_input(name: str, value) -> dict:
    return {
        "columns": [[value]],
        "rel_key": _rel_key(name, [_rel_typename(value)]),
        "type": "Relation"}


# `inputs`: map of parameter name to input value
def _query_action(model: str, inputs: dict = None, outputs: list = None) -> dict:
    inputs = inputs or {}
    inputs = [_query_action_input(k, v) for k, v in inputs.items()]
    return {
        "type": "QueryAction",
        "source": _model("query", model),
        "persist": [],
        "inputs": inputs,
        "outputs": outputs or []}


def _model(name: str, model: str) -> dict:
    return {
        "type": "Source",
        "name": name,
        "path": "",       # todo: check if required?
        "value": model}


# Returns full list of models.
def _list_models(ctx: Context, database: str, engine: str) -> dict:
    tx = Transaction(database, engine, mode=Mode.OPEN)
    rsp = tx.run(ctx, _list_action())
    actions = rsp["actions"]
    assert len(actions) == 1
    action = actions[0]
    models = action["result"]["sources"]
    return models


def create_database(ctx: Context, database: str, source: str = None) -> dict:
    data = {
        "name": database,
        "source_name": source}
    url = _mkurl(ctx, PATH_DATABASE)
    rsp = rest.put(ctx, url, data)
    return json.loads(rsp.read())


def delete_model(ctx: Context, database: str, engine: str, model: str) -> dict:
    tx = Transaction(database, engine, mode=Mode.OPEN, readonly=False)
    actions = [_delete_model_action(model)]
    return tx.run(ctx, *actions)


# Returns the named model
def get_model(ctx: Context, database: str, engine: str, name: str) -> str:
    models = _list_models(ctx, database, engine)
    for model in models:
        if model["name"] == name:
            return model["value"]
    raise Exception(f"model '{name}' not found")


def install_model(ctx: Context, database: str, engine: str, models: dict) -> dict:
    tx = Transaction(database, engine, mode=Mode.OPEN, readonly=False)
    actions = [_install_model_action(name, model)
               for name, model in models.items()]
    return tx.run(ctx, *actions)


def list_edbs(ctx: Context, database: str, engine: str) -> list:
    tx = Transaction(database, engine, mode=Mode.OPEN)
    rsp = tx.run(ctx, _list_edb_action())
    actions = rsp["actions"]
    assert len(actions) == 1
    action = actions[0]
    rels = action["result"]["rels"]
    return rels


# Returns a list of models installed in the given database.
def list_models(ctx: Context, database: str, engine: str) -> list:
    models = _list_models(ctx, database, engine)
    return [model["name"] for model in models]


# Generate a rel literal relation for the given dict.
def _gen_literal_dict(items: dict) -> str:
    result = []
    for k, v in items:
        result.append(f"{_gen_literal(k)},{_gen_literal(v)}")
    return '{' + ';'.join(result) + '}'


# Generate a rel literal for the given list.
def _gen_literal_list(items: list) -> str:
    result = []
    for item in items:
        result.append(_gen_literal(item))
    return '{' + ','.join(result) + '}'


# Genearte a rel literal for the given string.
def _gen_literal_str(v: str) -> str:
    v = str(v)
    v = v.replace("'", "\\'")
    return f"'{v}'"


# Genrate a rel literal for the given value.
def _gen_literal(v) -> str:
    if isinstance(v, bool):
        return str(v).lower()
    if isinstance(v, str):
        return _gen_literal_str(v)
    if isinstance(v, dict):
        return _gen_literal_dict(v)
    if isinstance(v, list):
        return _gen_literal_list(v)
    return repr(v)


_syntax_options = [
    "header",
    "header_row",
    "delim",
    "quotechar",
    "escapechar"]


# Generate list of config syntax options for `load_csv`.
def _gen_syntax_config(syntax: dict = {}) -> str:
    result = ""
    for k in _syntax_options:
        v = syntax.get(k, None)
        if v is not None:
            result += f"def config:syntax:{k}={_gen_literal(v)}\n"
    return result


# `syntax`:
#   * header: a map from col number to name (base 1)
#   * header_row: row number of header, 0 means no header (default: 1)
#   * delim: default: ,
#   * quotechar: default: "
#   * escapechar: default: \
#
# Schema: a map from col name to rel type name, eg:
#   {'a': "int", 'b': "string"}
def load_csv(ctx: Context, database: str, engine: str, relation: str,
             data: str or io.TextIOBase, syntax: dict = {}) -> dict:
    if isinstance(data, str):
        pass  # ok
    elif isinstance(data, io.TextIOBase):
        data = data.read()
    else:
        raise TypeError(f"bad type for arg 'data': {data.__class__.__name__}")
    inputs = {'data': data}
    command = _gen_syntax_config(syntax)
    command += ("def config:data = data\n"
                "def insert:%s = load_csv[config]" % relation)
    return exec_v1(ctx, database, engine, command, inputs=inputs, readonly=False)


def load_json(ctx: Context, database: str, engine: str, relation: str,
              data: str or io.TextIOBase) -> dict:
    if isinstance(data, str):
        pass  # ok
    elif isinstance(data, io.TextIOBase):
        data = data.read()
    else:
        raise TypeError(f"bad type for arg 'data': {data.__class__.__name__}")
    inputs = {'data': data}
    command = ("def config:data = data\n"
               "def insert:%s = load_json[config]" % relation)
    return exec_v1(ctx, database, engine, command, inputs=inputs, readonly=False)

def exec_v1(ctx: Context, database: str, engine: str, command: str,
          inputs: dict = None, readonly: bool = True) -> dict:
    tx = Transaction(database, engine, readonly=readonly)
    return tx.run(ctx, _query_action(command, inputs=inputs))

# Answers if the given transaction state is a terminal state.
def is_txn_term_state(state: str) -> bool:
    return state == "COMPLETED" or state == "ABORTED"

def exec(ctx: Context, database: str, engine: str, command: str,
          inputs: dict = None, readonly: bool = True) -> list:
    async_result = exec_async(ctx, database, engine, command, readonly=readonly)
    if isinstance(async_result, list):  # in case of if short-path, return results directly, no need to poll for state
        return async_result

    rsp = []
    while True:
        time.sleep(3)
        txn = get_transaction(ctx, async_result["id"])
        if is_txn_term_state(txn["state"]):
            rsp.append(txn)
            rsp.append(get_transaction_metadata(ctx, txn["id"]))
            rsp.append(get_transaction_problems(ctx, txn["id"]))
            rsp.append(get_transaction_results(ctx, txn["id"]))
            break

    return rsp


def exec_async(ctx: Context, database: str, engine: str, command: str,
                readonly: bool = True, inputs: dict = None) -> Union[dict, list]:
    tx = TransactionAsync(database, engine, readonly=readonly)
    return tx.run(ctx, command, inputs=inputs)


create_compute = create_engine  # deprecated, use create_engine
delete_compute = delete_engine  # deprecated, use delete_engine
get_compute = get_engine        # deprecated, use get_engine
list_computes = list_engines    # deprecated, use list_engines
list_edb = list_edbs            # deprecated, use list_edbs
delete_source = delete_model    # deprecated, use delete_model
get_source = get_model          # deprecated, use get_model
list_sources = list_models      # deprecated, use list_models
