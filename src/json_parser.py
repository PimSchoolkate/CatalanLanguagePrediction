# import pandas as pd


class JSONParser:
    '''
    Class for parsing json structures into dictionaries with "column: value" structure. The json is expected to not be
    homogenous in their structure. This parser recursively travels down the json and treats each node differently
    depending on whether it is a list or a dictionary.
    In order to find the values and the columns for the final dictionary, keys have to be provided that allow the parser
    to find the columns and values in the structure.

    For example, given the json structure:
    { r: { 'c': 'Name'
           'v': 'Jane Doe'
        }
    }
    The parser needs to have specified 'c' as the json_column_name and 'v' as the json_value_name.
    Json_column_name can also be passed a list if there is multiple keys that refer to the name of the column. The
    parser will concatenate all 'hits' for each value, meaning that you might end up with very lengthy names.

    It is possible to apply a mapping to the keys and the values by passing the mapping function to the respective
    in class mapping objects.

    '''
    class Container:
        def __init__(self, name):
            self.name = name
            self._dict = {}
            # self._pandas = None

        # def _assemble_pandas(self):
        #     self._pandas = pd.DataFrame(self._dict)

        def _add_kv(self, key, value):
            self._dict[key] = value

        def _key_exists(self, key):
            return key in self._dict.keys()

        def _add_key_value(self, key_value):
            key, value = key_value
            if self._key_exists(key):
                v = self._dict[key]
                if type(v) in [str, int, float]:
                    self._dict[key] = [v, value]
                elif type(v) == list:
                    self._dict[key].append(value)
                elif type(v) == tuple:
                    self._dict[key] = list(v)
                    self._dict[key].append(value)
                elif type(v) == dict:
                    print('Value is dict, which should not be the case')
                else:
                    print(f"Value type '{type(v)}' not supported")
            else:
                self._add_kv(key, value)

    class Holder:
        def __init__(self):
            self._keys = {}
            self._value = None

        def key(self, _ref, key):
            self._keys[_ref] = key

        def pop(self, key):
            self._keys.pop(key)

        def value(self, val):
            self._value = val

        def dump(self):
            val = self._value
            self._value = None
            return "_".join(self.get_keys()), val

        def get_keys(self):
            return self._keys.values()

        def does_not_have_value(self):
            return self._value is None

        def satisfied(self):
            return len(self._keys) != 0 and self._value is not None

    def __init__(self, json_value, json_column_name: tuple):
        self._json_data = {}
        self._containers = {}
        self._level = 0
        self._json_value_key = json_value
        self._json_column_keys = json_column_name
        self._key_map_fun = empty_map_func
        self._value_map_fun = empty_map_func

    def add_json(self, d, name):
        self._json_data[name] = [False, d]

    def parse_data(self):
        self._parse_all()

    def containers(self):
        return {name: con._dict for name, con in self._containers.items()}

    def add_key_map(self, fun):
        self._key_map_fun = fun

    def add_value_map(self, fun):
        self._value_map_fun = fun

    def names(self):
        return list(self._json_data.keys())

    def _parse_all(self):
        _to_parse = [(_name, v[1]) for i, (_name, v) in enumerate(self._json_data.items()) if v[0] is False]
        for _name, _data in _to_parse:
            self._current_container = self.Container(_name)
            self._holder = self.Holder()
            self._parse(_data, _level=self._level)
            self._containers[_name] = self._current_container

    def _parse(self, _j, _level):
        _level = _level + 1
        if type(_j) == dict:
            self._parse_dict(_j, _level)
        elif type(_j) == list:
            self._parse_list(_j, _level)

        if _level in self._holder._keys:
            self._holder.pop(_level)

    def _parse_dict(self, _d, _level):
        inter = intersect(self._json_column_keys, _d.keys())
        if len(inter) == 1:
            self._holder.key(_level, self._key_map_fun(_d[inter[0]]))
        elif len(inter) == 0:
            pass
        else:
            self._holder.key(_level, self._key_map_fun(" ".join([_d[int] for int in inter])))

        if self._json_value_key in _d.keys():
            if self._holder.does_not_have_value():
                self._holder.value(self._value_map_fun(_d[self._json_value_key]))
                if self._holder.satisfied():
                    self._current_container._add_key_value(self._holder.dump())

        for _k, _v in _d.items():
            self._parse(_v, _level)

    def _parse_list(self, _l, _level):
        for _e in _l:
            self._parse(_e, _level)


def intersect(lst1, lst2):
    return list(set(lst1) & set(lst2))


def homogenize_key(key):
    return key.replace(" ", "_")


def value_splitter(value):
    assert type(value) == str
    val = value.split(',')
    return val[0]


def empty_map_func(_in):
    return _in
