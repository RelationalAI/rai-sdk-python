import json
import unittest
import os
import uuid
import tempfile

from pathlib import Path
from railib import api, config


# Get creds from env vars if exists
client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")
client_credentials_url = os.getenv("CLIENT_CREDENTIALS_URL")
host = os.getenv("HOST")
custom_headers = json.loads(os.getenv('CUSTOM_HEADERS', '{}'))

if client_id is None:
    print("using config from path")
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
    host={host}
    """)
    file.seek(0)
    cfg = config.read(fname=file.name)
    file.close()

ctx = api.Context(**cfg)

suffix = uuid.uuid4()
engine = f"python-sdk-{suffix}"
dbname = f"python-sdk-{suffix}"


class TestTransactionAsync(unittest.TestCase):
    def setUp(self):
        rsp = api.create_engine_wait(ctx, engine, headers=custom_headers)
        self.assertEqual("PROVISIONED", rsp["state"])
        rsp = api.create_database(ctx, dbname)
        self.assertEqual("CREATED", rsp["database"]["state"])

    def test_v2_exec(self):
        cmd = "x, x^2, x^3, x^4 from x in {1; 2; 3; 4; 5}"
        rsp = api.exec(ctx, dbname, engine, cmd)

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
                'v1': [1, 2, 3, 4, 5],
                'v2': [1, 4, 9, 16, 25],
                'v3': [1, 8, 27, 64, 125],
                'v4': [1, 16, 81, 256, 625]
            },
            rsp.results[0]["table"].to_pydict())

    def test_models(self):
        models = api.list_models(ctx, self.dbname, self.engine)
        self.assertTrue(len(models) > 0)

        models = {'test_model': 'def foo=:bar'}
        resp = api.install_models(ctx, self.dbname, self.engine, models)
        self.assertEqual(resp.transaction['state'], 'COMPLETED')

        value = api.get_model(ctx, self.dbname, self.engine, 'test_model')
        self.assertEqual(models['test_model'], value)

        models = api.list_models(ctx, self.dbname, self.engine)
        self.assertTrue('test_model' in models)

        resp = api.delete_models(ctx, self.dbname, self.engine, ['test_model'])
        self.assertEqual(resp.transaction['state'], 'COMPLETED')

        models = api.list_models(ctx, self.dbname, self.engine)
        self.assertFalse('test_model' in models)

    def tearDown(self):
        api.delete_engine(ctx, self.engine)
        api.delete_database(ctx, self.dbname)


if __name__ == '__main__':
    unittest.main()
