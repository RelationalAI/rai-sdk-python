from time import sleep
import unittest
import os
import uuid
import tempfile

from pathlib import Path
from railib import api, config

# TODO: create_engine_wait should be added to API
# with exponential backoff


def create_engine_wait(ctx: api.Context, engine: str):
    state = api.create_engine(ctx, engine)["compute"]["state"]

    count = 0
    while not ("PROVISIONED" == state):
        if count > 12:
            return

        count += 1
        sleep(30)
        state = api.get_engine(ctx, engine)["state"]


# Get creds from env vars if exists
client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")
client_credentials_url = os.getenv("CLIENT_CREDENTIALS_URL")

if client_id is None:
    cfg = config.read()
else:
    file = tempfile.NamedTemporaryFile(mode="w")
    file.writelines(f"""
    [default]
    client_id={client_id}
    client_secret={client_secret}
    client_credentials_url={client_credentials_url}
    region=us-east
    port=443
    host=azure.relationalai.com
    """)
    file.seek(0)
    cfg = config.read(fname=file.name)
    file.close()

ctx = api.Context(**cfg)


class TestTransactionAsync(unittest.TestCase):
    def setUp(self):
        print("==> setup 1")
        suffix = uuid.uuid4()
        self.engine = f"python-sdk-{suffix}"
        self.dbname = f"python-sdk-{suffix}"

        create_engine_wait(ctx, self.engine)
        api.create_database(ctx, self.dbname)

    def test_v2_exec(self):
        print("=> test v2 exec")
        cmd = "x, x^2, x^3, x^4 from x in {1; 2; 3; 4; 5}"
        rsp = api.exec(ctx, self.dbname, self.engine, cmd)

        # transaction
        self.assertEqual("COMPLETED", rsp.transaction["state"])

        # metadata
        with open(os.path.join(Path(__file__).parent, "metadata.pb"), "rb") as f:
            data = f.read()
            self.assertEqual(
                rsp.metadata,
                api._parse_metadata_proto(data)
            )

        # problems
        self.assertEqual(0, len(rsp.problems))

        # results
        self.assertEqual(
            {
                'v1': [
                    1, 2, 3, 4, 5], 'v2': [
                    1, 4, 9, 16, 25], 'v3': [
                    1, 8, 27, 64, 125], 'v4': [
                        1, 16, 81, 256, 625]}, rsp.results[0]["table"].to_pydict())

    def tearDown(self):
        print("==> tear down 1")
        api.delete_engine(ctx, self.engine)
        api.delete_database(ctx, self.dbname)


class TestDataload(unittest.TestCase):
    def setUp(self):
        print("==> setup 2")
        suffix = uuid.uuid4()
        self.engine = f"python-sdk-{suffix}"
        self.dbname = f"python-sdk-{suffix}"
        print(self.engine)
        print(self.dbname)

        create_engine_wait(ctx, self.engine)
        api.create_database(ctx, self.dbname)

    def test_load_json(self):
        print("=> test load json")
        json = '{ "test" : 123 }'
        resp = api.load_json(ctx, self.dbname, self.engine, 'test_relation', json)
        self.assertEqual("COMPLETED", resp.transaction["state"])

        resp = api.exec(ctx, self.dbname, self.engine, 'def output = test_relation')
        self.assertEqual("COMPLETED", resp.transaction["state"])
        self.assertEqual({'v1': [123]}, resp.results[0]["table"].to_pydict())

    def test_load_csv(self):
        print("=> test load csv")
        csv = 'foo,bar\n1,2'
        resp = api.load_csv(ctx, self.dbname, self.engine, 'test_relation', csv)
        self.assertEqual("COMPLETED", resp.transaction["state"])

        resp = api.exec(ctx, self.dbname, self.engine, 'def output = test_relation')
        self.assertEqual("COMPLETED", resp.transaction["state"])
        self.assertEqual({'v1': [2], 'v2': ['2']}, resp.results[0]["table"].to_pydict())
        self.assertEqual({'v1': [2], 'v2': ['1']}, resp.results[1]["table"].to_pydict())

    def test_load_csv_with_syntax(self):
        print("=> load csv with syntax")
        csv = 'foo|bar\n1,2'
        resp = api.load_csv(
            ctx,
            self.dbname,
            self.engine,
            'test_relation',
            csv,
            {
                'header': {1: 'foo', 2: 'bar'},
                'delim': '|',
                'quotechar': "'",
                'header_row': 0,
                'escapechar': ']'
            }
        )
        self.assertEqual("COMPLETED", resp.transaction["state"])

        resp = api.exec(ctx, self.dbname, self.engine, 'def output = test_relation')
        self.assertEqual("COMPLETED", resp.transaction["state"])
        self.assertEqual({'v1': [2], 'v2': [2], 'v3': ['1,2']}, resp.results[0]["table"].to_pydict())

    def test_load_csv_with_schema(self):
        print("=> load csv with schema")
        csv = 'foo,bar\n1,test'
        resp = api.load_csv(
            ctx,
            self.dbname,
            self.engine,
            'test_relation',
            csv,
            schema={
                ':foo': 'int',
                ':bar': 'string'
            }
        )
        self.assertEqual("COMPLETED", resp.transaction["state"])

        resp = api.exec(ctx, self.dbname, self.engine, 'def output = test_relation')
        self.assertEqual("COMPLETED", resp.transaction["state"])
        self.assertEqual({'v1': [2], 'v2': ['test']}, resp.results[0]["table"].to_pydict())
        self.assertEqual({'v1': [2], 'v2': [1]}, resp.results[1]["table"].to_pydict())

    def tearDown(self):
        print("==> tear down 2")
        print(self.engine)
        print(self.dbname)
        api.delete_engine(ctx, self.engine)
        api.delete_database(ctx, self.dbname)


if __name__ == '__main__':
    unittest.main()
