# Copyright 2021 RelationalAI, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Object for navigating RAI transaction results."""

from typing import List


def _isstr(val) -> bool:
    return isinstance(val, str) and val[0] != ':'


def _show_row(row: list, end='\n'):
    row = [f'"{item}"' if _isstr(item) else str(item) for item in row]
    row = ', '.join(row)
    print(row, end=end)


# Represents an individual physical relation.
class Relation(dict):
    def __init__(self, item: dict):
        assert item.get("type", None) == "Relation"
        self._item = item
        self._schema = None

    def __getitem__(self, key):
        return self._item.__getitem__(key)

    def __repr__(self):
        return self._item.__repr__()

    @property
    def columns(self):
        return self["columns"]

    def get_row(self, ix: int) -> List:
        return [col[ix] for col in self.columns]

    @property
    def name(self):
        return self["rel_key"]["name"]

    @property
    def row_count(self):
        cols = self.columns
        return 0 if len(cols) == 0 else len(cols[0])

    @property
    def rows(self):
        for i in range(self.row_count):
            yield self.get_row(i)

    @property
    def schema(self):
        if self._schema is None:
            rkey = self._item["rel_key"]
            assert rkey["type"] == "RelKey"
            keys = rkey["keys"]
            vals = rkey["values"]
            self._schema = keys + vals
        return self._schema

    def show(self):
        schema = '*'.join(self.schema)
        print(f"// {self.name} ({schema})")
        nrows = 0
        for row in self.rows:
            if nrows > 0:
                print(";")
            _show_row(row, end='')
            nrows += 1
        print("true") if nrows == 0 else print()


class Results(dict):
    def __init__(self, data: dict):
        assert data.get("type") == "TransactionResult"
        dict.__init__(self, data)
        self._relations = None

    @property
    def aborted(self):
        return self.get("aborted", False)

    @property
    def relations(self):
        if self._relations is None:
            self._relations = [Relation(item) for item in self["output"]]
        return self._relations

    @ property
    def problems(self):
        return self.get("problems", [])  # todo: accessor for problems

    # Print query results as individual physical relations.
    def _show_relations(self) -> None:
        count = 0
        for relation in self.relations:
            if relation.name == "abort" and relation.row_count == 0:
                continue  # ignore constraint results
            if count > 0:
                print()
            relation.show()
            count += 1

    def show(self) -> None:
        if self.aborted:
            print("aborted")
            return
        if len(self.relations) == 0:
            print("false")
            return
        self._show_relations()
