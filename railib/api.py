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
import pyarrow as pa
import time
import re
import io
import logging
from enum import Enum, unique
from typing import Dict, List, Union
from requests_toolbelt import multipart
from . import rest

from .pb.message_pb2 import MetadataInfo


PATH_ENGINE = "/compute"
PATH_DATABASE = "/database"
PATH_TRANSACTION = "/transaction"
PATH_TRANSACTIONS = "/transactions"
PATH_USER = "/users"
PATH_OAUTH_CLIENT = "/oauth-clients"

# logger
logger = logging.getLogger(__package__)


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
    # engines (duplicate to computes)
    CREATE_ENGINE = "create:engine"
    DELETE_ENGINE = "delete:engine"
    LIST_ENGINES = "list:engine"
    READ_ENGINE = "read:engine"
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
]


# Context contains the state required to make rAI API calls.
class Context(rest.Context):
    def __init__(
        self,
        host: str = None,
        port: str = None,
        scheme: str = None,
        region: str = None,
        credentials=None,
        audience: str = None,
        retries: int = 0,
    ):
        super().__init__(region=region, credentials=credentials, retries=retries)
        self.host = host
        self.port = port or "443"
        self.scheme = scheme or "https"
        self.audience = audience


# Transaction async response class


class TransactionAsyncResponse:
    def __init__(
        self,
        transaction: dict = None,
        metadata: MetadataInfo = None,
        results: list = None,
        problems: list = None,
    ):
        self.transaction = transaction
        self.metadata = metadata
        self.results = results
        self.problems = problems

    def __str__(self):
        return str(
            {
                "transaction": self.transaction,
                "metadata": self.metadata,
                "results": self.results,
                "problems": self.problems,
            }
        )


# Transaction async file class


class TransactionAsyncFile:
    def __init__(
        self,
        name: str = None,
        filename: str = None,
        content_type: str = None,
        content: bytes = None,
    ):
        self.name = name
        self.filename = filename
        self.content_type = content_type
        self.content = content

    def __str__(self):
        return str(
            {
                "name": self.name,
                "filename": self.filename,
                "content-type": self.content_type,
                "content": "...",
            }
        )


# Construct a URL from the given context and path.


def _mkurl(ctx: Context, path: str) -> str:
    return f"{ctx.scheme}://{ctx.host}:{ctx.port}{path}"


# Retrieve an individual resource.
def _get_resource(ctx: Context, path: str, key=None, **kwargs) -> Dict:
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


# Parse "multipart/form-data" response


def _parse_multipart_form(
    content_type: str, content: bytes
) -> List[TransactionAsyncFile]:
    result = []

    parts = multipart.decoder.MultipartDecoder(
        content_type=content_type, content=content
    ).parts

    for part in parts:
        txn_file = TransactionAsyncFile()
        txn_file.content_type = part.headers[b"Content-Type"].decode()
        txn_file.content = part.content

        disposition = part.headers[b"Content-Disposition"]
        name = re.match(b'.*; name="(.+?)"', disposition)
        if not (name is None):
            txn_file.name = name.group(1).decode()
        filename = re.match(b'.*filename="(.+?)"', disposition)
        if not (filename is None):
            txn_file.filename = name.group(1).decode()

        result.append(txn_file)

    return result


# Parse TransactionAsync response


def _parse_transaction_async_response(
    files: List[TransactionAsyncFile],
) -> TransactionAsyncResponse:
    txn_file = next(iter([file for file in files if file.name == "transaction"]), None)
    metadata_file = next(
        iter([file for file in files if file.name == "metadata.proto"]), None
    )
    problems_file = next(
        iter([file for file in files if file.name == "problems"]), None
    )

    if txn_file is None:
        raise Exception("transaction part is missing")
    if metadata_file is None:
        raise Exception("metadata.proto part is missing")
    if problems_file is None:
        raise Exception("problems part is missing")

    txn = json.loads(txn_file.content)
    metadata = _parse_metadata_proto(metadata_file.content)
    results = _parse_arrow_results(files)
    problems = json.loads(problems_file.content)

    return TransactionAsyncResponse(txn, metadata, results, problems)


# Parse Metadata from protobuf


def _parse_metadata_proto(data: bytes) -> MetadataInfo:
    metadata = MetadataInfo()
    metadata.ParseFromString(data)
    return metadata


# Extract arrow result from transaction async files


def _parse_arrow_results(files: List[TransactionAsyncFile]):
    results = []
    result_files = [
        file
        for file in files
        if file.content_type == "application/vnd.apache.arrow.stream"
    ]

    for file in result_files:
        with pa.ipc.open_stream(file.content) as reader:
            schema = reader.schema
            batches = [batch for batch in reader]
            table = pa.Table.from_batches(batches=batches, schema=schema)
            results.append({"relationId": file.name, "table": table})
    return results

# polling with specified overhead
# delay is the overhead % of the time the transaction has been running so far


def poll_with_specified_overhead(
    f,
    overhead_rate: float,
    start_time: int = time.time(),
    timeout: int = None,
    max_tries: int = None,
    max_delay: int = 120,
):
    tries = 0
    max_time = time.time() + timeout if timeout else None

    while True:
        if f():
            break

        if max_tries is not None and tries >= max_tries:
            raise Exception(f'max tries {max_tries} exhausted')

        if max_time is not None and time.time() >= max_time:
            raise Exception(f'timed out after {timeout} seconds')

        tries += 1
        duration = min((time.time() - start_time) * overhead_rate, max_delay)
        if tries == 1:
            time.sleep(0.5)
        else:
            time.sleep(duration)


def is_engine_term_state(state: str) -> bool:
    return state == "PROVISIONED" or ("FAILED" in state)


def create_engine(ctx: Context, engine: str, size: str = "XS", **kwargs):
    data = {"region": ctx.region, "name": engine, "size": size}
    url = _mkurl(ctx, PATH_ENGINE)
    rsp = rest.put(ctx, url, data, **kwargs)
    return json.loads(rsp.read())


def create_engine_wait(ctx: Context, engine: str, size: str = "XS", **kwargs):
    create_engine(ctx, engine, size, **kwargs)
    poll_with_specified_overhead(
        lambda: is_engine_term_state(get_engine(ctx, engine)["state"]),
        overhead_rate=0.2,
        timeout=30 * 60,
    )
    return get_engine(ctx, engine)


def create_user(ctx: Context, email: str, roles: List[Role] = None, **kwargs):
    rs = roles or []
    data = {"email": email, "roles": [r.value for r in rs]}
    url = _mkurl(ctx, PATH_USER)
    rsp = rest.post(ctx, url, data, **kwargs)
    return json.loads(rsp.read())


def create_oauth_client(ctx: Context, name: str, permissions: List[Permission] = None, **kwargs):
    ps = permissions or []
    data = {"name": name, "permissions": ps}
    url = _mkurl(ctx, PATH_OAUTH_CLIENT)
    rsp = rest.post(ctx, url, data, **kwargs)
    return json.loads(rsp.read())["client"]


# Derives the database open_mode based on the given arguments.
def _create_mode(source_database: str, overwrite: bool) -> Mode:
    if source_database is not None:
        result = Mode.CLONE_OVERWRITE if overwrite else Mode.CLONE
    else:
        result = Mode.CREATE_OVERWRITE if overwrite else Mode.CREATE
    return result


def delete_database(ctx: Context, database: str, **kwargs) -> Dict:
    data = {"name": database}
    url = _mkurl(ctx, PATH_DATABASE)
    rsp = rest.delete(ctx, url, data, **kwargs)
    return json.loads(rsp.read())


def delete_engine(ctx: Context, engine: str, **kwargs) -> Dict:
    data = {"name": engine}
    url = _mkurl(ctx, PATH_ENGINE)
    rsp = rest.delete(ctx, url, data, **kwargs)
    return json.loads(rsp.read())


def delete_user(ctx: Context, id: str, **kwargs) -> Dict:
    url = _mkurl(ctx, f"{PATH_USER}/{id}")
    rsp = rest.delete(ctx, url, None, **kwargs)
    return json.loads(rsp.read())


def disable_user(ctx: Context, userid: str, **kwargs) -> Dict:
    return update_user(ctx, userid, status="INACTIVE", **kwargs)


def delete_oauth_client(ctx: Context, id: str, **kwargs) -> Dict:
    url = _mkurl(ctx, f"{PATH_OAUTH_CLIENT}/{id}")
    rsp = rest.delete(ctx, url, None, **kwargs)
    return json.loads(rsp.read())


def enable_user(ctx: Context, userid: str, **kwargs) -> Dict:
    return update_user(ctx, userid, status="ACTIVE", **kwargs)


def get_engine(ctx: Context, engine: str, **kwargs) -> Dict:
    return _get_resource(ctx, PATH_ENGINE, name=engine, deleted_on="", key="computes", **kwargs)


def get_database(ctx: Context, database: str, **kwargs) -> Dict:
    return _get_resource(ctx, PATH_DATABASE, name=database, key="databases", **kwargs)


def get_oauth_client(ctx: Context, id: str, **kwargs) -> Dict:
    return _get_resource(ctx, f"{PATH_OAUTH_CLIENT}/{id}", key="client", **kwargs)


def cancel_transaction(ctx: Context, id: str, **kwargs) -> Dict:
    rsp = rest.post(ctx, _mkurl(ctx, f"{PATH_TRANSACTIONS}/{id}/cancel"), {}, **kwargs)
    return json.loads(rsp.read())


def get_transaction(ctx: Context, id: str, **kwargs) -> Dict:
    return _get_resource(ctx, f"{PATH_TRANSACTIONS}/{id}", key="transaction", **kwargs)


def get_transaction_metadata(ctx: Context, id: str, **kwargs) -> List:
    headers = {"Accept": "application/x-protobuf"}
    url = _mkurl(ctx, f"{PATH_TRANSACTIONS}/{id}/metadata")
    rsp = rest.get(ctx, url, headers=headers, **kwargs)
    content_type = rsp.headers.get("content-type", "")
    if "application/x-protobuf" in content_type:
        return _parse_metadata_proto(rsp.read())

    raise Exception(f"invalid content type for metadata proto: {content_type}")


def get_transaction_problems(ctx: Context, id: str, **kwargs) -> List:
    return _get_collection(ctx, f"{PATH_TRANSACTIONS}/{id}/problems", **kwargs)


def get_transaction_results(ctx: Context, id: str, **kwargs) -> List:
    url = _mkurl(ctx, f"{PATH_TRANSACTIONS}/{id}/results")
    rsp = rest.get(ctx, url, **kwargs)
    content_type = rsp.headers.get("content-type", "")
    if "multipart/form-data" in content_type:
        parts = _parse_multipart_form(content_type, rsp.read())
        return _parse_arrow_results(parts)

    raise Exception("invalid response type")


# When problems are part of the results relations, this function should be
# deprecated, get_transaction_results should be called instead
def get_transaction_results_and_problems(ctx: Context, id: str, **kwargs) -> List:
    rsp = TransactionAsyncResponse()
    rsp.problems = get_transaction_problems(ctx, id, **kwargs)
    rsp.results = get_transaction_results(ctx, id, **kwargs)
    return rsp


def get_transaction_query(ctx: Context, id: str, **kwargs) -> str:
    url = _mkurl(ctx, f"{PATH_TRANSACTIONS}/{id}/query")
    rsp = rest.get(ctx, url, **kwargs)
    return rsp.read().decode("utf-8")


def list_transactions(ctx: Context, **kwargs) -> List:
    return _get_collection(ctx, PATH_TRANSACTIONS, key="transactions", **kwargs)


def get_user(ctx: Context, userid: str, **kwargs) -> Dict:
    return _get_resource(ctx, f"{PATH_USER}/{userid}", name=userid, **kwargs)


def list_engines(ctx: Context, state=None) -> List:
    kwargs = {}
    if state is not None:
        kwargs["state"] = state
    return _get_collection(ctx, PATH_ENGINE, key="computes", **kwargs)


def list_databases(ctx: Context, state=None) -> List:
    kwargs = {}
    if state is not None:
        kwargs["state"] = state
    return _get_collection(ctx, PATH_DATABASE, key="databases", **kwargs)


def list_users(ctx: Context, **kwargs) -> List:
    return _get_collection(ctx, PATH_USER, key="users", **kwargs)


def list_oauth_clients(ctx: Context, **kwargs) -> List:
    return _get_collection(ctx, PATH_OAUTH_CLIENT, key="clients", **kwargs)


def update_user(ctx: Context, userid: str, status: str = None, roles=None, **kwargs):
    data = {}
    if status is not None:
        data["status"] = status
    if roles is not None:
        data["roles"] = roles
    url = _mkurl(ctx, f"{PATH_USER}/{userid}")
    rsp = rest.patch(ctx, url, data, **kwargs)
    return json.loads(rsp.read())


class Transaction(object):
    def __init__(
        self,
        database: str,
        engine: str,
        abort=False,
        mode: Mode = Mode.OPEN,
        nowait_durable=False,
        readonly=False,
        source_database=None,
    ):
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
            actions.append(
                {
                    "name": f"action{i}",
                    "type": "LabeledAction",
                    "action": action,
                }
            )
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
            "version": 0,
        }
        if self.engine is not None:
            result["computeName"] = self.engine
        if self.source_database is not None:
            result["source_dbname"] = self.source_database
        return result

    def run(self, ctx: Context, *args) -> Dict:
        data = self.data
        data["actions"] = self._actions(args)
        # several of the request params are duplicated in the query
        kwargs = {
            "dbname": self.database,
            "compute_name": self.engine,
            "open_mode": self.mode.value,
            "region": ctx.region,
        }
        if self.source_database:
            kwargs["source_dbname"] = self.source_database
        url = _mkurl(ctx, PATH_TRANSACTION)
        rsp = rest.post(ctx, url, data, **kwargs)
        return json.loads(rsp.read())


class TransactionAsync(object):
    def __init__(
        self, database: str, engine: str, nowait_durable=False, readonly=False
    ):
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

    def run(self, ctx: Context, command: str, language: str, inputs: dict = None, **kwargs) -> Union[dict, list]:
        data = self.data
        data["query"] = command
        data["language"] = language
        if inputs is not None:
            inputs = [_query_action_input(k, v) for k, v in inputs.items()]
            data["v1_inputs"] = inputs
        rsp = rest.post(ctx, _mkurl(ctx, PATH_TRANSACTIONS), data, **kwargs)
        content_type = rsp.headers.get("content-type", None)
        content = rsp.read()
        # todo: response model should be based on status code (200 v. 201)
        # async mode
        if content_type.lower() == "application/json":
            return json.loads(content)
        # sync mode
        if "multipart/form-data" in content_type.lower():
            return _parse_multipart_form(content_type, content)
        raise Exception("invalid response type")


def _delete_model_action(name: str) -> Dict:
    return {"type": "ModifyWorkspaceAction", "delete_source": [name]}


def _install_model_action(name: str, model: str) -> Dict:
    return {"type": "InstallAction", "sources": [_model(name, model)]}


def _list_action():
    return {"type": "ListSourceAction"}


def _list_edb_action():
    return {"type": "ListEdbAction"}


# Return rel key correponding to the given name and list of keys.
def _rel_key(name: str, keys: list) -> Dict:
    return {"type": "RelKey", "name": name, "keys": keys, "values": []}


# Return the rel typename corresponding to the type of the given value.
def _rel_typename(v):
    if isinstance(v, str):
        return "RAI_VariableSizeStrings.VariableSizeString"
    raise TypeError("unsupported input type: {v.__class__.__name__")  # todo


# Return a qeury action input corresponding to the given name, value pair.
def _query_action_input(name: str, value) -> Dict:
    return {
        "columns": [[value]],
        "rel_key": _rel_key(name, [_rel_typename(value)]),
        "type": "Relation",
    }


# `inputs`: map of parameter name to input value
def _query_action(model: str, inputs: dict = None, outputs: list = None) -> Dict:
    inputs = inputs or {}
    inputs = [_query_action_input(k, v) for k, v in inputs.items()]
    return {
        "type": "QueryAction",
        "source": _model("query", model),
        "persist": [],
        "inputs": inputs,
        "outputs": outputs or [],
    }


def _model(name: str, model: str) -> Dict:
    return {
        "type": "Source",
        "name": name,
        "path": "",  # todo: check if required?
        "value": model,
    }


# Returns full list of models.
def _list_models(ctx: Context, database: str, engine: str) -> Dict:
    tx = Transaction(database, engine, mode=Mode.OPEN)
    rsp = tx.run(ctx, _list_action())
    actions = rsp["actions"]
    assert len(actions) == 1
    action = actions[0]
    models = action["result"]["sources"]
    return models


def create_database(ctx: Context, database: str, source: str = None, **kwargs) -> Dict:
    data = {"name": database, "source_name": source}
    url = _mkurl(ctx, PATH_DATABASE)
    rsp = rest.put(ctx, url, data, **kwargs)
    return json.loads(rsp.read())


def delete_model(ctx: Context, database: str, engine: str, model: str) -> Dict:
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


def install_model(ctx: Context, database: str, engine: str, models: dict) -> Dict:
    tx = Transaction(database, engine, mode=Mode.OPEN, readonly=False)
    actions = [_install_model_action(name, model) for name, model in models.items()]
    return tx.run(ctx, *actions)


def list_edbs(ctx: Context, database: str, engine: str) -> List:
    tx = Transaction(database, engine, mode=Mode.OPEN)
    rsp = tx.run(ctx, _list_edb_action())
    actions = rsp["actions"]
    assert len(actions) == 1
    action = actions[0]
    rels = action["result"]["rels"]
    return rels


# Returns a list of models installed in the given database.
def list_models(ctx: Context, database: str, engine: str) -> List:
    models = _list_models(ctx, database, engine)
    return [model["name"] for model in models]


# Generate a rel literal relation for the given dict.
def _gen_literal_dict(items: dict) -> str:
    result = []
    for k, v in items:
        result.append(f"{_gen_literal(k)},{_gen_literal(v)}")
    return "{" + ";".join(result) + "}"


# Generate a rel literal for the given list.
def _gen_literal_list(items: list) -> str:
    result = []
    for item in items:
        result.append(_gen_literal(item))
    return "{" + ",".join(result) + "}"


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


_syntax_options = ["header", "header_row", "delim", "quotechar", "escapechar"]


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
def load_csv(
    ctx: Context,
    database: str,
    engine: str,
    relation: str,
    data: str or io.TextIOBase,
    syntax: dict = {},
) -> Dict:
    if isinstance(data, str):
        pass  # ok
    elif isinstance(data, io.TextIOBase):
        data = data.read()
    else:
        raise TypeError(f"bad type for arg 'data': {data.__class__.__name__}")
    inputs = {"data": data}
    command = _gen_syntax_config(syntax)
    command += "def config:data = data\n" "def insert:%s = load_csv[config]" % relation
    return exec_v1(ctx, database, engine, command, inputs=inputs, readonly=False)


def load_json(
    ctx: Context,
    database: str,
    engine: str,
    relation: str,
    data: str or io.TextIOBase,
) -> Dict:
    if isinstance(data, str):
        pass  # ok
    elif isinstance(data, io.TextIOBase):
        data = data.read()
    else:
        raise TypeError(f"bad type for arg 'data': {data.__class__.__name__}")
    inputs = {"data": data}
    command = "def config:data = data\n" "def insert:%s = load_json[config]" % relation
    return exec_v1(ctx, database, engine, command, inputs=inputs, readonly=False)


def exec_v1(
    ctx: Context,
    database: str,
    engine: str,
    command: str,
    inputs: dict = None,
    readonly: bool = True,
) -> Dict:
    tx = Transaction(database, engine, readonly=readonly)
    return tx.run(ctx, _query_action(command, inputs=inputs))


# Answers if the given transaction state is a terminal state.
def is_txn_term_state(state: str) -> bool:
    return state == "COMPLETED" or state == "ABORTED"


def exec(
    ctx: Context,
    database: str,
    engine: str,
    command: str,
    inputs: dict = None,
    readonly: bool = True,
    **kwargs
) -> TransactionAsyncResponse:
    logger.info('exec: database %s engine %s readonly %s' % (database, engine, readonly))
    start_time = time.time()
    txn = exec_async(ctx, database, engine, command, inputs=inputs, readonly=readonly)
    logger.debug('exec: transaction id - %s' % txn.transaction["id"])

    # in case of if short-path, return results directly, no need to poll for state
    if not (txn.results is None):
        return txn

    logger.debug('exec: polling for transaction with id - %s' % txn.transaction["id"])
    rsp = TransactionAsyncResponse()
    txn = get_transaction(ctx, txn.transaction["id"], **kwargs)

    poll_with_specified_overhead(
        lambda: is_txn_term_state(get_transaction(ctx, txn["id"], **kwargs)["state"]),
        overhead_rate=0.2,
        start_time=start_time,
    )

    rsp.transaction = get_transaction(ctx, txn["id"], **kwargs)
    rsp.metadata = get_transaction_metadata(ctx, txn["id"], **kwargs)
    rsp.problems = get_transaction_problems(ctx, txn["id"], **kwargs)
    rsp.results = get_transaction_results(ctx, txn["id"], **kwargs)

    return rsp


def exec_async(
    ctx: Context,
    database: str,
    engine: str,
    command: str,
    readonly: bool = True,
    inputs: dict = None,
    language: str = "",
    **kwargs,
) -> TransactionAsyncResponse:
    tx = TransactionAsync(database, engine, readonly=readonly)
    rsp = tx.run(ctx, command, language=language, inputs=inputs, **kwargs)

    if isinstance(rsp, dict):
        return TransactionAsyncResponse(rsp, None, None, None)

    return _parse_transaction_async_response(rsp)


create_compute = create_engine  # deprecated, use create_engine
delete_compute = delete_engine  # deprecated, use delete_engine
get_compute = get_engine  # deprecated, use get_engine
list_computes = list_engines  # deprecated, use list_engines
list_edb = list_edbs  # deprecated, use list_edbs
delete_source = delete_model  # deprecated, use delete_model
get_source = get_model  # deprecated, use get_model
list_sources = list_models  # deprecated, use list_models
