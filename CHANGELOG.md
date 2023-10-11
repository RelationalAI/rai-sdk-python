# Changelog

## v0.6.17

* Removed unnecessary dependencies of the SDK, bumped Pyarrow to v10.


## v0.6.16

* Expanded the retry mechanism for HTTP failures raised as `ConnectionError`.

## v0.6.15

* Increase auth token expiration buffer from 5s to 60s.

## v0.6.14

* Added a retry mechanism for HTTP failures raised as `URLError`.
    * Defaults to `0` retries
    * Configurable through `Context` (example `Context(**cfg, retries=3)` to set retries to 3)

## v0.6.13

* Improved debug logging for `exec`.

## v0.6.12

* `create_engine` and `create_engine_wait` accept engine size as a string (e.g. "XS", "M", etc).

## v0.6.11

* Log warnings if failed to read or write to the local access token cache.

## v0.6.10

* Fix for `ImportError: cannot import name 'appengine' from 'urllib3.contrib'`
* Run tests using multiple python versions `[3.7, 3.8, 3.9]`

## v0.6.9

* Added `poll_with_specified_overhead`
* Added `create_engine_wait`
* OAuth access token caching on disk `~/.rai/tokens.js`
* Access key authentication support dropped

## v0.6.8

* Added protobuf metadata support
* Deprecated json metadata
* `exec` and `exec_async` return `TransactionAsyncResponse`
* Added integration tests for asynchronous transaction
* Setup CI workflow

## v0.6.4

* Additional user APIs and examples:
    - api.update_user
    - api.enable_user
    - fixed inputs for `exec_async`
    - added `exec` and `exec_async` to run v2 transactions
    - added `get_transaction` to get v2 transaction details
    - added `get_transaction_metadata` to get v2 transaction metadata
    - added `get_transaction_results_and_problems` to get v2 transaction results and problems

## v0.6.3

* Rename `source` to `model`
* Rename api.list_edb to api.list_edbs
* Implement create/list/get/delete oauth clients
* Implement create/disable users
* Implement delete database by name

## v0.6.2

* Add api.load_csv
* Add api.load_json
* Add inputs arg to api.query

## v0.6.1

* Automate publishing to PyPi

## v0.6.0

* Renamed `compute` to `engine`
* Ensure all modules have an __all__ definition
* Update api constants to use Python3 Enums
* Improve error handling in examples
* Fix api.get_user to match new endpoint signature
* Add state filter param to list_databases
* Fix config example in README.md
