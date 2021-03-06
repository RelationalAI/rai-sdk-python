# Changelog

## main

* Additional user APIs and examples:
    - api.update_user
    - api.enable_user

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
