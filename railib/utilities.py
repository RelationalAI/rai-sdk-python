# Copyright 2022 RelationalAI, Inc.
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
# limitations under the License

"""Helpers for reshaping query output."""

__all__ = [
    "to_json"
]

__SYMBOL_PREFIX = ':'
__ARRAY_MARKER = '[]'
__ARRAY_SYMBOL = __SYMBOL_PREFIX + __ARRAY_MARKER
__EMPTY_ARRAY_MARKER = 'Missing'


def to_json(output: list) -> dict:
    '''
    to_json converts Rel relations to dict object.
    '''

    def nested_set(root, keys, value):
        '''
        set a value in a dict given a list of nested keys. 
        '''
        for i in range(len(keys) - 1):
            key = keys[i]
            next_value = {} if type(keys[i+1]) is str else []
            if type(root) is dict:
                root = root.setdefault(key, next_value)
            elif type(root) is list:
                if len(root) <= key:  # add pad to the array
                    root.extend([None] * (key + 1 - len(root)))
                if root[key] is None:
                    root[key] = next_value
                root = root[key]

        if type(root) is list and len(root) <= keys[-1]:
            root.extend([None] * (keys[-1] + 1 - len(root)))
        root[keys[-1]] = value

    output = list(filter(lambda relation: len(relation['rel_key']['keys']) > 0 or len(
        relation['rel_key']['values']) > 0, output))

    if len(output) == 1 and not output[0]['rel_key']['keys'][0][0] == __SYMBOL_PREFIX:
        return output[0]['columns'][0][0]

    root_array_number = len(list(filter(
        lambda relation: relation['rel_key']['keys'][0] == __ARRAY_SYMBOL, output)))

    if root_array_number > 0 and root_array_number < len(output):
        raise Exception("toJSON: Inconsistent root array relations.")

    result = {} if root_array_number == 0 else []

    for relation in output:
        keys = relation['rel_key']['keys'] + relation['rel_key']['values']
        if len(keys) == 0:
            return
        prop_path: list = []
        index: int = 0
        column_lookup: dict = {}

        for i in range(len(keys) - 1):
            key = keys[i]
            if key[0] == __SYMBOL_PREFIX:
                prop_path.append(key[1:])
            else:
                if(keys[i-1] == __ARRAY_SYMBOL):
                    column_lookup[index] = len(prop_path) - 1
                else:
                    column_lookup[index] = len(prop_path)
                    prop_path.append(key)
                index += 1

        for i in range(len(relation['columns'][0])):
            path_to_set = prop_path.copy()
            value = None

            for j in range(len(relation['columns'])):
                col_value = relation['columns'][j][i]
                path_index = column_lookup.get(j, None)
                if path_index != None:
                    is_array = path_to_set[path_index] == __ARRAY_MARKER
                    if(is_array):
                        array_index = col_value - \
                            1 if type(col_value) is int else i
                        path_to_set[path_index] = array_index
                    else:
                        path_to_set[path_index] = str(col_value)
                elif j == len(relation['columns']) - 1:
                    value = col_value

            if path_to_set[-1] == __ARRAY_MARKER and keys[-1] == __EMPTY_ARRAY_MARKER:
                path_to_set = path_to_set[:-1]
                value = []
            elif value == None:
                value = {}

            if len(path_to_set) > 0:
                nested_set(result, path_to_set, value)

    return result
