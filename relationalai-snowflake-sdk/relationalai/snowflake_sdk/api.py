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
    return session.sql(f"SELECT RAI.GET_RAI_DATABASE('{database}')")

def list_databases(session: Session) -> DataFrame:
    return session.sql("""SELECT
        value:id::string as ID
        ,value:account_name::string as ACCOUNT_NAME
        ,value:created_by::string as CREATED_BY
        ,value:name::string as NAME
        ,value:region::string as REGION
        ,value:state::string as STATE
      FROM
        (SELECT RAI.LIST_RAI_DATABASES() as LRD)
        ,LATERAL FLATTEN (input => LRD)""")

def use_database(session: Session, database: str) -> DataFrame:
    return session.sql(f"CALL RAI.USE_RAI_DATABASE('{database}')")

def create_engine(session: Session, engine: str, size: str = 'S') -> DataFrame:
    return session.sql(f"SELECT RAI.CREATE_RAI_ENGINE('{engine}', '{size}')")

def delete_engine(session: Session, engine: str) -> DataFrame:
    return session.sql(f"SELECT RAI.DELETE_RAI_ENGINE('{engine}')")

def get_engine(session: Session, engine: str) -> DataFrame:
    return session.sql(f"SELECT RAI.GET_RAI_ENGINE('{engine}')")

def list_engines(session: Session) -> DataFrame:
    return session.sql(f"SELECT RAI.LIST_RAI_ENGINES()")

def use_engine(session: Session, engine: str) -> DataFrame:
    return session.sql(f"CALL RAI.USE_RAI_ENGINE('{engine}')")

def exec(session: Session, database: str, engine: str, query: str) -> DataFrame:
    return session.sql(f"SELECT RAI.EXEC('{database}', '{engine}', '{query}', null, true)")

# def exec(session: Session, query: str) -> DataFrame:
#     return session.sql(f"SELECT RAI.EXEC('{query}')")
