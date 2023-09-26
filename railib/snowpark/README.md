# The RelationalAI Software Development Kit for Python in Snowflake Snowpark

| Workflow | Status |
| --------------------------- | ---------------------------------------------------------------------- |
| Continuous Integration (CI) | ![build](https://github.com/RelationalAI/rai-sdk-python/actions/workflows/build.yaml/badge.svg) |
| [Publish to PYPI](https://pypi.org/project/rai-sdk/) | ![publish](https://github.com/RelationalAI/rai-sdk-python/actions/workflows/publish.yaml/badge.svg) |

The RelationalAI (RAI) SDK for Python enables developers to access the RAI
REST APIs from Snowpark Python.

* You can find RelationalAI Python SDK documentation at <https://docs.relational.ai/rkgms/sdk/python-sdk>
* You can find RelationalAI product documentation at <https://docs.relational.ai>
* You can learn more about RelationalAI at <https://relational.ai>

## Getting started

### Requirements

* RelationalAI Native App installed in Snowflake


### Installing the SDK locally

Install using pip:

```console
$ [sudo] pip install rai-sdk-snowpark

```

Install from source using pip:

```console
$ git clone git@github.com:RelationalAI/rai-sdk-python.git
$ cd rai-sdk-python/railib/snowflake
$ [sudo] python3 setup.py install
```

Install from source in `editable` mode.

```console
$ git clone git@github.com:RelationalAI/rai-sdk-python.git
$ cd rai-sdk-python/railib/snowflake
$ [sudo] pip install -r requirements.txt
$ [sudo] pip install -e .
```