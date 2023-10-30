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
    "create_data_stream",
    "delete_data_stream",
    "get_data_stream",
    "get_data_stream_status",
    "list_data_streams",
    "ping",
]


#################################
# Setup
#################################


def ping(session: Session) -> List[Row]:
    return session.sql("select PING() as result").collect()


def use_schema(session: Session, searchPath: str) -> List[Row]:
    return session.sql(f"alter session set SEARCH_PATH = '$current, $public, {searchPath}'").collect()

#################################
# DATABASE
#################################


def create_database(session: Session, database: str) -> List[Row]:
    return session.sql(f"select CREATE_RAI_DATABASE('{database}') as status").collect()


def delete_database(session: Session, database: str) -> List[Row]:
    return session.sql(f"select DELETE_RAI_DATABASE('{database}') as status").collect()


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
            (select GET_RAI_DATABASE('{database}') as res);
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
            (select LIST_RAI_DATABASES() as res),
            lateral flatten (input => res)
    """)


def use_database(session: Session, database: str) -> List[Row]:
    res = session.sql(f"call USE_RAI_DATABASE('{database}')").collect()

    if res[0][0] != database:
        rsp = json.loads(res[0][0])

        if not rsp["success"]:
            raise Exception(rsp["message"])

    return res


def get_current_database(session: Session) -> DataFrame:
    return session.sql("select CURRENT_RAI_DATABASE() as current_database")


#################################
# ENGINE
#################################

def create_engine(session: Session, engine: str, size: str = 'XS') -> List[Row]:
    return session.sql(f"select CREATE_RAI_ENGINE('{engine}', '{size}') as status").collect()


def delete_engine(session: Session, engine: str) -> List[Row]:
    return session.sql(f"select DELETE_RAI_ENGINE('{engine}') as status").collect()


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
            (select GET_RAI_ENGINE('{engine}') as res);
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
            (select LIST_RAI_ENGINES() as res),
            lateral flatten (input => res);
""")


def use_engine(session: Session, engine: str) -> List[Row]:
    res = session.sql(f"CALL USE_RAI_ENGINE('{engine}')").collect()

    if res[0][0] != engine:
        rsp = json.loads(res[0][0])

        if not rsp["success"]:
            raise Exception(rsp["message"])

    return res


def get_current_engine(session: Session) -> DataFrame:
    return session.sql("select CURRENT_RAI_ENGINE() as current_engine")


#################################
# Transaction
#################################


def exec(session: Session, database: str, engine: str, query: str, data=None, readonly: bool = True) -> DataFrame:
    return session.sql(f"select EXEC('{database}', '{engine}', '{query}', {data if data else 'null'}, {readonly})")


def exec_into(session: Session, database: str, engine: str, query: str, warehouse: str, fq_target: str, data=None, readonly: bool = True) -> List[Row]:
    return session.sql(f"select EXEC_INTO('{database}', '{engine}', '{query}', {data if data else 'null' }, {readonly}, '{warehouse}', '{fq_target}') as status").collect()


#################################
# Model
#################################

def load_model(session: Session, database: str, engine: str, name: str, path: str) -> List[Row]:
    return session.sql(f"select LOAD_MODEL('{database}', '{engine}', '{name}', '{path}')").collect()


def load_model_code(session: Session, database: str, engine: str, name: str, code: str) -> List[Row]:
    return session.sql(f"select LOAD_MODEL_CODE('{database}', '{engine}', '{name}', '{code}')").collect()


#################################
# Data Stream
#################################


def create_data_stream(session: Session, data_source: str, database: str, relation: str) -> List[Row]:
    return session.sql(f"call CREATE_DATA_STREAM('{data_source}', '{database}', '{relation}')").collect()


def delete_data_stream(session: Session, data_source: str) -> List[Row]:
    return session.sql(f"call DELETE_DATA_STREAM('{data_source}')").collect()


def get_data_stream(session: Session, data_source_fq_name: str) -> DataFrame:
    return session.sql(f"""
        select
            res:id::string            as id,
            res:account::string       as account,
            res:createdBy::string     as created_by,
            res:createdOn::string     as created_on,
            res:dbLink::string        as database_link,
            res:integration::string   as integration,
            res:name::string          as name,
            res:state::string         as state,
            res:rai::object           as rai,
            res:snowflake::object     as snowflake
        from
            (select GET_DATA_STREAM('{data_source_fq_name}') as res)
    """)


def get_data_stream_status(session: Session, data_source_name: str) -> DataFrame:
    return session.sql(f"call GET_DATA_STREAM_STATUS('{data_source_name}')")


def list_data_streams(session: Session) -> DataFrame:
    return session.sql(f"""
        select
            value:id::string            as id,
            value:account::string       as account,
            value:createdBy::string     as created_by,
            value:createdOn::string     as created_on,
            value:dbLink::string        as database_link,
            value:integration::string   as integration,
            value:name::string          as name,
            value:state::string         as state,
            value:rai::object           as rai,
            value:snowflake::object     as snowflake
        from
            (select LIST_DATA_STREAMS() as res),
            lateral flatten (input => res)
    """)
