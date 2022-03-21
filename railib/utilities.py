
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
            nextValue = {} if type(keys[i+1]) is str else []
            if type(root) is dict:
                root = root.setdefault(key, nextValue)
            elif type(root) is list:
                if len(root) <= key : #add pad to the array 
                    root.extend([None]* (key + 1 - len(root)))
                if root[key] is None:
                    root[key] = nextValue
                root = root[key]
        
        if type(root) is list and len(root) <= keys[-1]:
            root.extend([None]* (keys[-1] + 1 - len(root)))
        root[keys[-1]] = value
            
   
    output = list(filter(lambda relation: len(relation['rel_key']['keys']) > 0 or  len(relation['rel_key']['values']) > 0, output))
    
    if len(output) == 1 and not output[0]['rel_key']['keys'][0][0] == __SYMBOL_PREFIX:
        return output[0]['columns'][0][0]
    

    rootArrayNumber = len(list(filter(lambda relation: relation['rel_key']['keys'][0] == __ARRAY_SYMBOL, output)))

    if rootArrayNumber > 0 and rootArrayNumber < len(output):
        raise Exception("toJSON: Inconsistent root array relations.")

    result = {} if rootArrayNumber == 0 else []

    for relation in output:
        keys = relation['rel_key']['keys'] + relation['rel_key']['values']
        if len(keys) == 0:
            return
        propPath: list = []
        index: int = 0
        columnLookup: dict = {}

        for i in range(len(keys) - 1):
            key = keys[i]
            if key[0] == __SYMBOL_PREFIX:
                propPath.append(key[1:])
            else:
                if(keys[i-1] == __ARRAY_SYMBOL):
                    columnLookup[index] = len(propPath) - 1
                else:
                    columnLookup[index] = len(propPath)
                    propPath.append(key)
                index += 1
        

        for i in range(len(relation['columns'][0])):
            pathToSet = propPath.copy()
            value = None

            for j in range(len(relation['columns'])):
                colVal = relation['columns'][j][i]
                pathIndex = columnLookup.get(j, None)
                if pathIndex != None:
                    isArray = pathToSet[pathIndex] == __ARRAY_MARKER
                    if(isArray):
                        arrayIndex = colVal - 1 if type(colVal) is int else i
                        pathToSet[pathIndex] = arrayIndex
                    else:
                        pathToSet[pathIndex] = str(colVal)
                elif j == len(relation['columns']) -1:
                    value = colVal

            if pathToSet[-1] == __ARRAY_MARKER and keys[-1] == __EMPTY_ARRAY_MARKER:
                pathToSet = pathToSet[:-1]
                value = []
            elif value == None:
                value = {}

            if len(pathToSet) > 0:
                nested_set(result, pathToSet, value)

    return result
