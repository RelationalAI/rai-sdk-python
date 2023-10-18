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
from snowflake.snowpark import Session
from snowflake.snowpark.dataframe import DataFrame

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
]


def create_database(session: Session, database: str) -> bool:
    create_db_res = session.sql(f"SELECT RAI.CREATE_RAI_DATABASE('{database}')").collect()

    return create_db_res[0][0] == '"ok"'


def delete_database(session: Session, database: str) -> bool:
    delete_db_res = session.sql(f"SELECT RAI.DELETE_RAI_DATABASE('{database}')").collect()

    return delete_db_res[0][0] == '"ok"'


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
            (select RAI.LIST_RAI_DATABASES() as lrd),
            lateral flatten (input => lrd)
    """)


def use_database(session: Session, database: str) -> DataFrame:
    res = session.sql(f"CALL RAI.USE_RAI_DATABASE('{database}')").collect()

    if res[0][0] != database:
        rsp = json.loads(res[0][0])

        if not rsp['success']:
            raise Exception(rsp['message'])


def create_engine(session: Session, engine: str, size: str = 'S') -> DataFrame:
    res = session.sql(f"SELECT RAI.CREATE_RAI_ENGINE('{engine}', '{size}')").collect()

    return res[0][0] == '"ok"'


def delete_engine(session: Session, engine: str) -> DataFrame:
    res = session.sql(f"SELECT RAI.DELETE_RAI_ENGINE('{engine}')").collect()

    return res[0][0] == '"ok"'


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


def use_engine(session: Session, engine: str) -> DataFrame:
    res = session.sql(f"CALL RAI.USE_RAI_ENGINE('{engine}')").collect()

    if res[0][0] != engine:
        rsp = json.loads(res[0][0])

        if not rsp['success']:
            raise Exception(rsp['message'])


def exec(session: Session, database: str, engine: str, query: str) -> DataFrame:
    return session.sql(f"SELECT RAI.EXEC('{database}', '{engine}', '{query}', null, true)")
