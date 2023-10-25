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

"""Operation level interface to the RelationalAI within Snowflake Snowpark"""

import json
import logging
from typing import List

from snowflake.snowpark import Session
from snowflake.snowpark.dataframe import DataFrame
from snowflake.snowpark.row import Row

# logger
logger = logging.getLogger(__package__)

__all__ = [
    "create_database",
    "create_engine",
    "delete_database",
    "delete_engine",
    "get_database",
    "get_engine",
    "list_databases",
    "list_engines",
    "use_database",
    "use_engine",
    "get_current_database",
    "get_current_engine",
    "exec",
    "exec_into",
    "load_data",
    "load_model",
    "load_model_code",
    "load_model_query",
    "create_data_stream",
    "delete_data_stream",
    "get_data_stream",
    "get_data_stream_status",
    "list_data_streams",
    "ping",
]


#################################
# DATABASE
#################################


def create_database(session: Session, database: str) -> List[Row]:
    return session.sql(f"select RAI.CREATE_RAI_DATABASE('{database}') as status").collect()


def delete_database(session: Session, database: str) -> List[Row]:
    return session.sql(f"select RAI.DELETE_RAI_DATABASE('{database}') as status").collect()


def get_database(session: Session, database: str) -> DataFrame:
    return session.sql(f"""
        select
            res:id::string            as id,
            res:account_name::string  as account_name,
            res:created_by::string    as created_by,
            res:name::string          as name,
            res:region::string        as region,
            res:state::string         as state
        from
            (select RAI.GET_RAI_DATABASE('{database}') as res);
    """)


def list_databases(session: Session) -> DataFrame:
    return session.sql("""
        select
            value:id::string           as id,
            value:account_name::string as account_name,
            value:created_by::string   as created_by,
            value:name::string         as name,
            value:region::string       as region,
            value:state::string        as state
        from
            (select RAI.LIST_RAI_DATABASES() as res),
            lateral flatten (input => res)
    """)


def use_database(session: Session, database: str) -> List[Row]:
    res = session.sql(f"call RAI.USE_RAI_DATABASE('{database}')").collect()

    if res[0][0] != database:
        rsp = json.loads(res[0][0])

        if not rsp["success"]:
            raise Exception(rsp["message"])

    return res


def get_current_database(session: Session) -> DataFrame:
    return session.sql("select RAI.CURRENT_RAI_DATABASE() as current_database")


#################################
# ENGINE
#################################

def create_engine(session: Session, engine: str, size: str = 'XS') -> List[Row]:
    return session.sql(f"select RAI.CREATE_RAI_ENGINE('{engine}', '{size}') as status").collect()


def delete_engine(session: Session, engine: str) -> List[Row]:
    return session.sql(f"select RAI.DELETE_RAI_ENGINE('{engine}') as status").collect()


def get_engine(session: Session, engine: str) -> DataFrame:
    return session.sql(f"""
        select
            res:id::string           as id,
            res:account_name::string as account_name,
            res:created_by::string   as created_by,
            res:created_on::string   as created_on,
            res:name::string         as name,
            res:region::string       as region,
            res:size::string         as size,
            res:state::string        as state
        from
            (select RAI.GET_RAI_ENGINE('{engine}') as res);
    """)


def list_engines(session: Session) -> DataFrame:
    return session.sql(f"""
        select
            value:id::string           as id,
            value:account_name::string as account_name,
            value:created_by::string   as created_by,
            value:created_on::string   as created_on,
            value:name::string         as name,
            value:region::string       as region,
            value:size::string         as size,
            value:state::string        as state
        from
            (select RAI.LIST_RAI_ENGINES() as res),
            lateral flatten (input => res);
""")


def use_engine(session: Session, engine: str) -> List[Row]:
    res = session.sql(f"CALL RAI.USE_RAI_ENGINE('{engine}')").collect()

    if res[0][0] != engine:
        rsp = json.loads(res[0][0])

        if not rsp["success"]:
            raise Exception(rsp["message"])

    return res


def get_current_engine(session: Session) -> DataFrame:
    return session.sql("select RAI.CURRENT_RAI_ENGINE() as current_engine")


#################################
# Transaction
#################################


def exec(session: Session, database: str, engine: str, query: str, data=None, readonly: bool = True) -> DataFrame:
    return session.sql(f"select RAI.EXEC('{database}', '{engine}', '{query}', {data if data else 'null'}, {readonly})")


def exec_into(session: Session, database: str, engine: str, query: str, warehouse: str, target: str, data=None, readonly: bool = True) -> DataFrame:
    return session.sql(f"select RAI.EXEC_INTO('{database}', '{engine}', '{query}', '{data if data else 'null'}', {readonly}, '{warehouse}', '{target}')")


#################################
# Model
#################################

def load_model(session: Session, database: str, engine: str, name: str, path: str) -> List[Row]:
    return session.sql(f"select RAI.LOAD_MODEL('{database}', '{engine}', '{name}', '{path}')").collect()


def load_model_code(session: Session, database: str, engine: str, name: str, code: str) -> List[Row]:
    return session.sql(f"select RAI.LOAD_MODEL_CODE('{database}', '{engine}', '{name}', '{code}')").collect()


def load_model_query(session: Session, name: str, path: str) -> List[Row]:
    return session.sql(f"select RAI.LOAD_MODEL_QUERY('{name}', '{path}')").collect()

#################################
# Data Stream
#################################


def create_data_stream(session: Session, data_source: str, database: str, base_relation: str) -> List[Row]:
    return session.sql(f"select RAI.CREATE_DATA_STREAM('{data_source}', '{database}', '{base_relation}') as status").collect()


def delete_data_stream(session: Session, data_source: str) -> List[Row]:
    return session.sql(f"select RAI.DELETE_DATA_STREAM('{data_source}') as status").collect()


def get_data_stream(session: Session, data_source: str) -> DataFrame:
    return session.sql(f"select RAI.GET_DATA_STREAM('{data_source}')")


def get_data_stream_status(session: Session, data_source: str) -> DataFrame:
    return session.sql(f"select RAI.GET_DATA_STREAM_STATUS('{data_source}') as status")


def list_data_streams(session: Session) -> DataFrame:
    return session.sql(f"select RAI.LIST_DATA_STREAMS()")

#################################
# Misc
#################################


def load_data(session: Session, database: str, relation: str, primary_key: str, query: str) -> List[Row]:
    return session.sql(f"select RAI.LOAD_DATA('{database}', '{relation}', '{primary_key}', '{query}')").collect()


def ping(session: Session) -> List[Row]:
    return session.sql("select RAI.PING() as result").collect()
