from logging.config import fileConfig
from typing import List
import json
import os
import unittest
import uuid

from snowflake.snowpark import Session
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.row import Row

from relationalai.snowflake_sdk import api


# Default Snowflake Session connection parameters
SF_USER = os.getenv("SF_USER")
SF_PASSWORD = os.getenv("SF_PASSWORD")
SF_ACCOUNT = os.getenv("SF_ACCOUNT")
SF_ROLE = os.getenv("SF_ROLE")
SF_WAREHOUSE = os.getenv("SF_WAREHOUSE")
SF_DATABASE = os.getenv("SF_DATABASE")
SF_SCHEMA = os.getenv("SF_SCHEMA")

# Fully qualified schema and schema where RAI is installed
FQ_SCHEMA = f"{SF_DATABASE}.{SF_SCHEMA}"
RAI_SCHEMA = f"{SF_DATABASE}.RAI"


connection_parameters = {
    "user": SF_USER,
    "password": SF_PASSWORD,
    "account": SF_ACCOUNT,
    "role": SF_ROLE,
    "warehouse": SF_WAREHOUSE,
    "database": SF_DATABASE,
    "schema": SF_SCHEMA,
}


suffix = uuid.uuid4()
engine_name = f"snowflake-python-sdk-{suffix}"
db_name = f"snowflake-python-sdk-{suffix}"

# init "rai" logger
fileConfig("./test/logger.config")


def generate_engine_name():
    return f"snowflake-python-sdk-eng-{uuid.uuid4()}"


def generate_db_name():
    return f"snowflake-python-sdk-db-{uuid.uuid4()}"


def check_status_ok(res: List[Row]):
    return res[0]["STATUS"] == '"ok"'


class TestDatabaseAPI(unittest.TestCase):
    DATABASE_RESPONSE_FIELDS = ["ID", "ACCOUNT_NAME", "CREATED_BY", "NAME", "REGION", "STATE"]

    @classmethod
    def setUpClass(cls):
        cls._session = Session.builder.configs(connection_parameters).create()
        api.use_schema(cls._session, RAI_SCHEMA)

    @classmethod
    def tearDownClass(cls):
        cls._session.close()

    def test_create_database(self):
        create_database_res = api.create_database(self._session, db_name)
        self.assertTrue(check_status_ok(create_database_res))

        with self.assertRaises(SnowparkSQLException):
            api.create_database(self._session, db_name)

    def test_delete_database_api(self):
        create_database_res = api.create_database(self._session, db_name)
        self.assertTrue(check_status_ok(create_database_res))

        delete_database_res = api.delete_database(self._session, db_name)
        self.assertTrue(check_status_ok(delete_database_res))

        delete_database_res = api.delete_database(self._session, "random-db-name-that-doesnt-exist")
        self.assertFalse(check_status_ok(delete_database_res))

    def test_list_databases(self):
        api.create_database(self._session, db_name)

        list_dbs_res = api.list_databases(self._session).collect()
        self.assertTrue(len(list_dbs_res) > 0)

        first_db = list_dbs_res[0]

        for field in self.DATABASE_RESPONSE_FIELDS:
            self.assertTrue(hasattr(first_db, field))

        api.delete_database(self._session, db_name)

    def test_get_database(self):
        api.create_database(self._session, db_name)

        get_db_res = api.get_database(self._session, db_name).collect()
        self.assertTrue(len(get_db_res) == 1)

        first_db = get_db_res[0]

        for field in self.DATABASE_RESPONSE_FIELDS:
            self.assertTrue(hasattr(first_db, field))

    def test_use_database(self):
        api.create_database(self._session, db_name)

        use_database_res = api.use_database(self._session, db_name)
        self.assertEqual(use_database_res[0][0], db_name)

        current_db_res = api.get_current_database(self._session).collect()
        self.assertEqual(current_db_res[0]["CURRENT_DATABASE"], db_name)

        with self.assertRaises(Exception):
            api.use_database(self._session, "random-db-name-that-doesnt-exist")

    def tearDown(self):
        api.delete_database(self._session, db_name)


@unittest.skip("Skipping until we implement a new engine API that follows the async pattern.")
class TestEngineAPI(unittest.TestCase):
    ENGINE_RESPONSE_FIELDS = ["ACCOUNT_NAME", "CREATED_BY", "CREATED_ON", "ID", "NAME", "REGION", "SIZE", "STATE"]

    def setUp(self):
        self.session = Session.builder.configs(connection_parameters).create()
        self.engine_name = generate_engine_name()

    def test_create_engine(self):
        create_engine_res = api.create_engine(self.session, self.engine_name)
        self.assertTrue(check_status_ok(create_engine_res))

        with self.assertRaises(SnowparkSQLException):
            api.create_engine(self.session, self.engine_name)

    def test_delete_engine(self):
        create_engine_res = api.create_engine(self.session, self.engine_name)
        self.assertTrue(check_status_ok(create_engine_res))

        delete_engine_res = api.delete_engine(self.session, self.engine_name)
        self.assertTrue(check_status_ok(delete_engine_res))

        delete_engine_res = api.delete_engine(self.session, "random-engine-name-that-doesnt-exist")
        self.assertFalse(check_status_ok(delete_engine_res))

    def test_list_engines(self):
        api.create_engine(self.session, self.engine_name)

        list_engines_res = api.list_engines(self.session).collect()
        self.assertTrue(len(list_engines_res) > 0)

        first_engine = list_engines_res[0]

        for field in self.ENGINE_RESPONSE_FIELDS:
            self.assertTrue(hasattr(first_engine, field))

    def test_get_engine(self):
        api.create_engine(self.session, self.engine_name)

        get_engine_res = api.get_engine(self.session, self.engine_name).collect()
        self.assertTrue(len(get_engine_res) == 1)

        first_engine = get_engine_res[0]

        for field in self.ENGINE_RESPONSE_FIELDS:
            self.assertTrue(hasattr(first_engine, field))

    def test_use_engine(self):
        api.create_engine(self.session, self.engine_name)

        use_engine_res = api.use_engine(self.session, self.engine_name)
        self.assertEqual(use_engine_res[0][0], self.engine_name)

        current_engine_res = api.get_current_engine(self.session).collect()
        self.assertEqual(current_engine_res[0]["CURRENT_ENGINE"], self.engine_name)

        with self.assertRaises(Exception):
            api.use_engine(self.session, "random-engine-name-that-doesnt-exist")

    def tearDown(self):
        api.delete_engine(self.session, self.engine_name)
        pass


class TestExecApi(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._session = Session.builder.configs(connection_parameters).create()
        cls._engine_name = generate_engine_name()
        cls._db_name = generate_db_name()

        api.use_schema(cls._session, RAI_SCHEMA)
        api.create_database(cls._session, cls._db_name)
        api.create_engine(cls._session, cls._engine_name)

    @classmethod
    def tearDownClass(cls):
        api.delete_database(cls._session, cls._db_name)
        api.delete_engine(cls._session, cls._engine_name)
        cls._session.close()

    def test_exec(self):
        res = json.loads(api.exec(self._session, self._db_name, self._engine_name, "def output = 1 + 1").collect()[0][0])

        self.assertEqual(res[0][0], 2)

    def test_exec_with_data(self):
        res = api.exec(
            self._session,
            self._db_name,
            self._engine_name,
            "def output = foo",
            {"foo": "hello"}
        ).collect()[0][0]

        self.assertEqual(json.loads(res)[0][0], "hello")

    def test_exec_into(self):
        table_name = f"{FQ_SCHEMA}.test_exec_into_table"
        res = api.exec_into(self._session, self._db_name, self._engine_name, "def output = 1 + 1", connection_parameters["warehouse"], table_name)
        self.assertTrue(check_status_ok(res))

        res = self._session.sql(f"select * from {table_name}").collect()
        self.assertEqual(res[0][0], "2")

    def test_exec_into_with_data(self):
        table_name = f"{FQ_SCHEMA}.test_exec_into_table"
        res = api.exec_into(
            self._session,
            self._db_name,
            self._engine_name,
            "def output = foo",
            connection_parameters["warehouse"],
            table_name,
            {"foo": "hello"}
        )
        self.assertTrue(check_status_ok(res))

        res = self._session.sql(f"select * from {table_name}").collect()
        self.assertEqual(res[0][0], "hello")


class TestModelApi(unittest.TestCase):
    def setUp(self) -> None:
        self.session = Session.builder.configs(connection_parameters).create()
        self.engine_name = generate_engine_name()
        self.db_name = generate_db_name()

        api.use_schema(self.session, RAI_SCHEMA)
        api.create_database(self.session, self.db_name)
        api.create_engine(self.session, self.engine_name)

    def test_load_model_code(self):
        res = api.load_model_code(self.session, self.db_name, self.engine_name, "my_model", "def mymax[x, y] = maximum[abs[x], abs[y]]")
        res = json.loads(api.exec(self.session, self.db_name, self.engine_name, "def output = mymax[5, -10]").collect()[0][0])

        self.assertEqual(res[0][0], 10)

    def tearDown(self) -> None:
        api.delete_database(self.session, self.db_name)
        api.delete_engine(self.session, self.engine_name)
        self.session.close()


class TestDataStreamApi(unittest.TestCase):
    DATA_STREAM_RESPONSE_FIELDS = ["ID", "NAME", "ACCOUNT", "CREATED_BY", "CREATED_ON", "RAI", "SNOWFLAKE", "STATE", "DATABASE_LINK", "INTEGRATION"]
    DATA_STREAM_TABLE_NAME = "my_edges"

    @classmethod
    def setUpClass(cls):
        cls._session = Session.builder.configs(connection_parameters).create()
        cls._engine_name = generate_engine_name()
        cls._db_name = generate_db_name()

        api.use_schema(cls._session, RAI_SCHEMA)
        api.create_database(cls._session, cls._db_name)
        api.create_engine(cls._session, cls._engine_name)
        api.use_database(cls._session, cls._db_name)
        api.use_engine(cls._session, cls._engine_name)

        cls._session.sql(f"CREATE OR REPLACE TABLE {cls.DATA_STREAM_TABLE_NAME}(x INT, y INT) AS SELECT * FROM VALUES (1, 2), (2, 3)").collect()

    @classmethod
    def tearDownClass(cls):
        api.delete_database(cls._session, cls._db_name)
        api.delete_engine(cls._session, cls._engine_name)
        cls._session.sql(f"DROP TABLE {cls.DATA_STREAM_TABLE_NAME}").collect()
        cls._session.close()

    def test_create_delete_data_stream(self):
        res = json.loads(api.create_data_stream(self._session, self.DATA_STREAM_TABLE_NAME, self._db_name, self.DATA_STREAM_TABLE_NAME)[0]["CREATE_DATA_STREAM"])
        self.assertIsNotNone(res["id"])
        self.assertIsNotNone(res["name"])
        self.assertEqual(res["state"], "CREATED")

        res = api.delete_data_stream(self._session, self.DATA_STREAM_TABLE_NAME)
        self.assertEqual(res[0]["DELETE_DATA_STREAM"], '"ok"')

    def test_get_data_stream(self):
        api.create_data_stream(self._session, self.DATA_STREAM_TABLE_NAME, self._db_name, self.DATA_STREAM_TABLE_NAME)

        res = api.get_data_stream(self._session, f"{FQ_SCHEMA}.{self.DATA_STREAM_TABLE_NAME}").collect()
        first_ds = res[0]

        for field in self.DATA_STREAM_RESPONSE_FIELDS:
            self.assertTrue(hasattr(first_ds, field))

        api.delete_data_stream(self._session, self.DATA_STREAM_TABLE_NAME)

    def test_list_data_stream(self):
        api.create_data_stream(self._session, self.DATA_STREAM_TABLE_NAME, self._db_name, self.DATA_STREAM_TABLE_NAME)

        res = api.list_data_streams(self._session).collect()
        first_ds = res[0]

        for field in self.DATA_STREAM_RESPONSE_FIELDS:
            self.assertTrue(hasattr(first_ds, field))

        api.delete_data_stream(self._session, self.DATA_STREAM_TABLE_NAME)


if __name__ == '__main__':
    unittest.main()
