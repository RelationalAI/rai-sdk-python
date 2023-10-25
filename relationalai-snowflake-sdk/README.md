# The RelationalAI Software Development Kit for Snowflake Snowpark

The RelationalAI (RAI) SDK for Python enables developers to access the RAI
REST APIs from Snowflake Snowpark.

* You can find RelationalAI Python SDK documentation at <https://docs.relational.ai/rkgms/sdk/python-sdk>
* You can find RelationalAI product documentation at <https://docs.relational.ai>
* You can learn more about RelationalAI at <https://relational.ai>

## Getting started

### Requirements

* Python 3.7+

### Installing the SDK

Install from source in `editable` mode.

```console
$ git clone git@github.com:RelationalAI/rai-sdk-python.git
$ cd rai-sdk-python
$ [sudo] pip install -e relationalai-snowflake-sdk
```

### Usage

Create a session according to [Snowpark API](https://docs.snowflake.com/en/developer-guide/snowpark/python/creating-session)

```python
from snowflake.snowpark import Session
from relationalai.snowflake_sdk import api


connection_parameters = {
    "user": "...",
    # and any other connection parameters
}

session = Session.builder.configs(connection_parameters).create()

api.use_schema(session, "DATABASE.SCHEMA where RelationalAI SQL Library is installed")

api.create_database(session, "my-database")
```

## Support

You can reach the RAI developer support team at `support@relational.ai`

## Contributing

We value feedback and contributions from our developer community. Feel free
to submit an issue or a PR here.

## License

The RelationalAI Software Development Kit for Python is licensed under the
Apache License 2.0. See:
https://github.com/RelationalAI/rai-sdk-python/blob/master/LICENSE

