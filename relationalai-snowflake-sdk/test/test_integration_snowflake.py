import unittest
import uuid
from typing import List
import json

from snowflake.snowpark import Session
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.row import Row

from logging.config import fileConfig
from relationalai.snowflake_sdk import api


connection_parameters = {
    "user": "",
    "password": "",
    "account": "",
    "database": "",
    "schema": "",
}


suffix = uuid.uuid4()
engine_name = f"snowflake-python-sdk-{suffix}"
db_name = f"snowflake-python-sdk-{suffix}"

# init "rai" logger
fileConfig("./test/logger.config")


def check_status_ok(res: List[Row]):
    return res[0]["STATUS"] == '"ok"'


class TestDatabaseAPI(unittest.TestCase):
    def setUp(self):
        self.session = Session.builder.configs(connection_parameters).create()
        api.use_schema(self.session, "SNOWFLAKE_INTEGRATION_SANDBOX.RAI")

    def test_create_database(self):
        create_database_res = api.create_database(self.session, db_name)
        self.assertTrue(check_status_ok(create_database_res))

        with self.assertRaises(SnowparkSQLException):
            api.create_database(self.session, db_name)

    def test_delete_database_api(self):
        create_database_res = api.create_database(self.session, db_name)
        self.assertTrue(check_status_ok(create_database_res))

        delete_database_res = api.delete_database(self.session, db_name)
        self.assertTrue(check_status_ok(delete_database_res))

        delete_database_res = api.delete_database(self.session, "random-db-name-that-doesnt-exist")
        self.assertFalse(check_status_ok(delete_database_res))

    def test_list_databases(self):
        api.create_database(self.session, db_name)

        list_dbs_res = api.list_databases(self.session).collect()
        self.assertTrue(len(list_dbs_res) > 0)

        first_db = list_dbs_res[0]

        self.assertTrue(hasattr(first_db, 'ID'))
        self.assertTrue(hasattr(first_db, 'ACCOUNT_NAME'))
        self.assertTrue(hasattr(first_db, 'CREATED_BY'))
        self.assertTrue(hasattr(first_db, 'NAME'))
        self.assertTrue(hasattr(first_db, 'REGION'))
        self.assertTrue(hasattr(first_db, 'STATE'))

        api.delete_database(self.session, db_name)

    def test_get_database(self):
        api.create_database(self.session, db_name)

        get_db_res = api.get_database(self.session, db_name).collect()
        self.assertTrue(len(get_db_res) == 1)

        first_db = get_db_res[0]

        self.assertTrue(hasattr(first_db, 'ID'))
        self.assertTrue(hasattr(first_db, 'ACCOUNT_NAME'))
        self.assertTrue(hasattr(first_db, 'CREATED_BY'))
        self.assertTrue(hasattr(first_db, 'NAME'))
        self.assertTrue(hasattr(first_db, 'REGION'))
        self.assertTrue(hasattr(first_db, 'STATE'))

    def test_use_database(self):
        api.create_database(self.session, db_name)

        use_database_res = api.use_database(self.session, db_name)
        self.assertEqual(use_database_res[0][0], db_name)

        current_db_res = api.get_current_database(self.session).collect()
        self.assertEqual(current_db_res[0]["CURRENT_DATABASE"], db_name)

        with self.assertRaises(Exception):
            api.use_database(self.session, "random-db-name-that-doesnt-exist")

    def tearDown(self):
        api.delete_database(self.session, db_name)
        self.session.close()


# Commenting out because the tests fail intermittently because of the SF timeout handling and SF external function retries.
# Can be uncommented once we modify the implementation of create engine API in the snoflake integration service to handle the timeout and retries  behavior of SF.
#
# class TestEngineAPI(unittest.TestCase):
#     def setUp(self):
#         self.session = session or Session.builder.configs(connection_parameters).create()
#         self.engine_name = f"snowflake-python-sdk-{uuid.uuid4()}"

#     def test_create_engine(self):
#         print("test_create_engine", self.engine_name)
#         create_engine_res = api.create_engine(self.session, self.engine_name)
#         self.assertTrue(check_status_ok(create_engine_res))

#         with self.assertRaises(SnowparkSQLException):
#             api.create_engine(self.session, self.engine_name)

#     def test_delete_engine(self):
#         print("test_delete_engine", self.engine_name)
#         create_engine_res = api.create_engine(self.session, self.engine_name)
#         self.assertTrue(check_status_ok(create_engine_res))

#         delete_engine_res = api.delete_engine(self.session, self.engine_name)
#         self.assertTrue(check_status_ok(delete_engine_res))

#         delete_engine_res = api.delete_engine(self.session, "random-engine-name-that-doesnt-exist")
#         self.assertFalse(check_status_ok(delete_engine_res))

#     def test_list_engines(self):
#         print("test_list_engines", self.engine_name)
#         api.create_engine(self.session, self.engine_name)

#         list_engines_res = api.list_engines(self.session).collect()
#         self.assertTrue(len(list_engines_res) > 0)

#         first_engine = list_engines_res[0]

#         self.assertTrue(hasattr(first_engine, 'ACCOUNT_NAME'))
#         self.assertTrue(hasattr(first_engine, 'CREATED_BY'))
#         self.assertTrue(hasattr(first_engine, 'CREATED_ON'))
#         self.assertTrue(hasattr(first_engine, 'ID'))
#         self.assertTrue(hasattr(first_engine, 'NAME'))
#         self.assertTrue(hasattr(first_engine, 'REGION'))
#         self.assertTrue(hasattr(first_engine, 'SIZE'))
#         self.assertTrue(hasattr(first_engine, 'STATE'))

#     def test_get_engine(self):
#         api.create_engine(self.session, self.engine_name)

#         get_engine_res = api.get_engine(self.session, self.engine_name).collect()
#         self.assertTrue(len(get_engine_res) == 1)

#         first_engine = get_engine_res[0]

#         self.assertTrue(hasattr(first_engine, 'ACCOUNT_NAME'))
#         self.assertTrue(hasattr(first_engine, 'CREATED_BY'))
#         self.assertTrue(hasattr(first_engine, 'CREATED_ON'))
#         self.assertTrue(hasattr(first_engine, 'ID'))
#         self.assertTrue(hasattr(first_engine, 'NAME'))
#         self.assertTrue(hasattr(first_engine, 'REGION'))
#         self.assertTrue(hasattr(first_engine, 'SIZE'))
#         self.assertTrue(hasattr(first_engine, 'STATE'))

#     def test_use_engine(self):
#         print("test_use_engine", self.engine_name)
#         api.create_engine(self.session, self.engine_name)

#         use_engine_res = api.use_engine(self.session, self.engine_name)
#         self.assertEqual(use_engine_res[0][0], self.engine_name)

#         current_engine_res = api.get_current_engine(self.session).collect()
#         self.assertEqual(current_engine_res[0]["CURRENT_ENGINE"], self.engine_name)

#         with self.assertRaises(Exception):
#             api.use_engine(self.session, "random-engine-name-that-doesnt-exist")

#     def tearDown(self):
#         api.delete_engine(self.session, self.engine_name)
#         pass


class TestExecApi(unittest.TestCase):
    def setUp(self) -> None:
        self.session = Session.builder.configs(connection_parameters).create()
        self.engine_name = f"snowflake-python-sdk-{uuid.uuid4()}"
        self.db_name = f"snowflake-python-sdk-{uuid.uuid4()}"

        api.use_schema(self.session, "SNOWFLAKE_INTEGRATION_SANDBOX.RAI")
        api.create_database(self.session, self.db_name)
        api.create_engine(self.session, self.engine_name)

    def test_exec(self):
        res = json.loads(api.exec(self.session, self.db_name, self.engine_name, "def output = 1 + 1").collect()[0][0])

        self.assertEqual(res[0][0], 2)

    def tearDown(self) -> None:
        api.delete_database(self.session, self.db_name)
        api.delete_engine(self.session, self.engine_name)
        self.session.close()


class TestModelApi(unittest.TestCase):
    def setUp(self) -> None:
        self.session = Session.builder.configs(connection_parameters).create()
        self.engine_name = f"snowflake-python-sdk-{uuid.uuid4()}"
        self.db_name = f"snowflake-python-sdk-{uuid.uuid4()}"

        api.use_schema(self.session, "SNOWFLAKE_INTEGRATION_SANDBOX.RAI")
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


if __name__ == '__main__':
    unittest.main()
