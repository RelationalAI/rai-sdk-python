# The RelationalAI Software Development Kit for Python

The RelationalAI (RAI) SDK for Python enables developers to access the RAI
REST APIs from Python.

* You can learn more about RelationalAI at <https://relational.ai>
* You can find RelationalAI documentation at <https://docs.relational.ai>

## Requirements

* Python 3.7+

### Installing the SDK

You can install from sources using pip:

```console
$ git clone git@github.com:RelationalAI/rai-sdk-python.git
$ cd rai-sdk-python
$ [sudo] python3 setup.py install
```

You can install from sources in `editable` mode. Instead of `[sudo] python setup.py install` use:

```console
$ [sudo] pip install -e .
```

Or you can use the sources directly by putting the repo on your `$PYTHONPATH`
by modifying your environment, or by running `source` on the `setenv` script.

```console
$ source ./setenv
```

If you run from sources directly, you will also have to manually install the
SDK dependencies:

```console
$ [sudo] pip install -f requirements.txt
```

## Create a configuration file

In order to run the examples and, you will need to create an SDK config file.
The default location for the file is `$HOME/.rai/config` and the file should
include the following:

Sample configurtion using OAuth client credentials:

```conf
[default]
host = azure.relationalai.com
port = <api-port>      # optional, default: 443
scheme = <scheme>      # optional, default: https
client_id = <your client_id>
client_secret = <your client secret>
client_credentials_url = <account login URL>  #optional
# default: https://login.relationalai.com/oauth/token
```

Sample configurtion using API access key credentials (deprecated):

```conf
[default]
host = azure.relationalai.com
port = <api-port>      # optional, default: 443
scheme = <scheme>      # optional, default: https
access_key = <your public access key>
private_key_filname = <name of file containing private key>
```

Note, the SDK expects to find the private key file in the same folder as the
config file.

You can copy `config.spec` from the root of this repo and modify as needed.

## Examples

Each of the example files in the `./examples` folder is standalone and can be
run from the command line, eg:

```console
$ cd examples
$ python3 ./list_computes.py
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
