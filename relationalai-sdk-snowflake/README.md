# The RelationalAI Software Development Kit for Snowflake Snowpark Python

The RelationalAI (RAI) SDK for Python enables developers to access the RAI
APIs from Snowflake Snowpark.

* You can find RelationalAI product documentation at <https://docs.relational.ai>
* You can find RelationalAI product documentation for Snowflake integration at <https://docs.relational.ai/preview/snowflake>
* You can learn more about RelationalAI at <https://relational.ai>

## Getting started

### Requirements

* Python 3.8+

### Installing the SDK

Install using pip:

```console
$ [sudo] pip install relationalai-sdk-snowflake
```

<!-- Install using conda:
```console
$ conda install -c https://repo.anaconda.com/pkgs/snowflake relationalai-sdk-snowflake
``` -->

Install from source in `editable` mode.

```console
$ git clone git@github.com:RelationalAI/rai-sdk-python.git
$ cd rai-sdk-python
$ [sudo] pip install -e relationalai-sdk-snowflake
```

### Usage

#### Prerequisites 

In order to use the SDK [RelationalAI integration with Snowflake](https://docs.relational.ai/preview/snowflake) needs to be enabled.

#### Usage

Create a session according to [Snowflake Snowpark API](https://docs.snowflake.com/en/developer-guide/snowpark/python/creating-session).

```python
from snowflake.snowpark import Session
from relationalai.snowflake_sdk import api


connection_parameters = {
    "user": "...",
    # and any other connection parameters
}

session = Session.builder.configs(connection_parameters).create()

# Required step for RelationalAI API to be discoverable from within you current database and schema.
# Provide database and schema where RelationalAI integration has been installed into.
api.use_schema(session, "<DATABASE>.<SCHEMA>")

# See the relationalai.snowflake_sdk.api module for the list of all available APIs.
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

