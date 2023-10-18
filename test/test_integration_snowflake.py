import unittest
import uuid

from snowflake.snowpark import Session

from logging.config import fileConfig
from relationalai.snowflake_sdk import api


connection_parameters = {
    "user": "anatoli.kurtsevich@relational.ai",
    "authenticator": "externalbrowser",
    "account": "NDSOEBE-DR75630",
    "database": "SNOWFLAKE_INTEGRATION_SANDBOX",
    "schema": "ANATOLI",
}


suffix = uuid.uuid4()
engine = f"sf-python-sdk-{suffix}"
dbname = f"sf-python-sdk-{suffix}"

# init "rai" logger
fileConfig("./test/logger.config")


class TestDatabase(unittest.TestCase):
    def setUp(self):
        self.session = Session.builder.configs(connection_parameters).create()

    def test_create_delete_database_api(self):
        create_database_res = api.create_database(self.session, dbname)
        self.assertTrue(create_database_res)

        delete_database_res = api.delete_database(self.session, dbname)
        self.assertTrue(delete_database_res)

    def test_create_database(self):
        create_database_res = api.create_database(self.session, dbname)
        self.assertTrue(create_database_res)

        with self.assertRaises(Exception):
            api.create_database(self.session, dbname)

    def test_list_databases(self):
        df_list_dbs = api.list_databases(self.session)
        list_dbs_res = df_list_dbs.collect()

        self.assertTrue(len(list_dbs_res) > 0)

        fist_el = list_dbs_res[0]

        self.assertTrue(hasattr(fist_el, 'ID'))
        self.assertTrue(hasattr(fist_el, 'ACCOUNT_NAME'))
        self.assertTrue(hasattr(fist_el, 'CREATED_BY'))
        self.assertTrue(hasattr(fist_el, 'NAME'))
        self.assertTrue(hasattr(fist_el, 'REGION'))
        self.assertTrue(hasattr(fist_el, 'STATE'))

    def test_get_database(self):
        api.create_database(self.session, dbname)

        df_get_db = api.get_database(self.session, dbname)
        get_db_res = df_get_db.collect()

        self.assertTrue(len(get_db_res) == 1)

        fist_el = get_db_res[0]

        self.assertTrue(hasattr(fist_el, 'ID'))
        self.assertTrue(hasattr(fist_el, 'ACCOUNT_NAME'))
        self.assertTrue(hasattr(fist_el, 'CREATED_BY'))
        self.assertTrue(hasattr(fist_el, 'NAME'))
        self.assertTrue(hasattr(fist_el, 'REGION'))
        self.assertTrue(hasattr(fist_el, 'STATE'))

    def test_use_database(self):
        api.create_database(self.session, dbname)

        self.assertIsNone(api.use_database(self.session, dbname))

        with self.assertRaises(Exception):
            api.use_database(self.session, 'some-random-db-that-doesnt-exist')

    def tearDown(self):
        api.delete_database(self.session, dbname)


if __name__ == '__main__':
    unittest.main()
