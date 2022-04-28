import json
import re
from collections.abc import Mapping

import pandas as pd
from flatten_dict import flatten

enumerate_type = (list, tuple)
flattenable_types = (Mapping,) + enumerate_type


def flatten_local(json_dict, result_dict={}):
    '''

    :param json_dict: The JSON data passed by user.
    :param result_dict:
    :return: The flattened json into dict datatype.
    '''
    result_dict.update(json_dict)
    json_dict = flatten(json_dict,
                        reducer='tuple',
                        max_flatten_depth=2,
                        enumerate_types=enumerate_type,
                        keep_empty_types=enumerate_type
                        )
    result_dict.update(json_dict)
    for key, value in json_dict.items():
        if isinstance(value, flattenable_types):
            flatten_local(json_dict, result_dict)
            break
    return result_dict


def flatten_tuple(key_list, flattened_tuple=()):
    '''

    :param key_list: Nested tuple hierarchy of key.
    :param flattened_tuple: Flattened tuple i.e. non nested tuple.
    :return:  Nested tuple of key hierarchy flattened into a tuple.
    '''
    for x in key_list:
        flattened_tuple += flatten_tuple(x, flattened_tuple) if isinstance(x, tuple) else (x,)
    return flattened_tuple


def find_element(input_json, search_key=None, search_value=None, regex=False):
    '''
    :param input_json: The JSON data passed by user.
    :param search_value: string/regex expression of the value that needs to be search within the values of the dictionary.
    :param search_key: string or regex expression of the key that needs to be search within the keys of the dictionary.
    :param regex: Set to true , it we need to find the results on regular expression provided.
    :return: The dict of hierarchy within json or None
    '''
    dumped_json = json.dumps(input_json)
    matched_elements = None
    input_json = json.loads(input_json)

    if not regex and ((search_key is not None and str(search_key) not in dumped_json) or (
            search_value is not None and str(search_value) not in dumped_json)):
        return matched_elements
    if search_key is not None:
        search_pattern = re.compile(search_key) if regex else re.compile(re.escape(search_key))
        flat_json = flatten_local(input_json, {(k,): v for k, v in input_json.items()})
        pd_df = pd.DataFrame(flat_json.items()).rename(columns={0: 'key', 1: 'value'})
        matched_df = pd_df.loc[(pd_df['key'].apply(
            lambda x: isinstance(x[-1], str) and (True if search_pattern.fullmatch(x[-1]) else False))), ['key',
                                                                                                          'value']]
        matched_elements = {flatten_tuple(v['key']): v['value'] for i, v in matched_df.iterrows()}
    elif search_value is not None:
        flat_json = flatten(input_json, enumerate_types=enumerate_type, keep_empty_types=enumerate_type)
        pd_df = pd.DataFrame(flat_json.items()).rename(columns={0: 'key', 1: 'value'})

        if isinstance(search_value, str):
            search_pattern = re.compile(search_value) if regex else re.compile(re.escape(search_value))
            matched_df = pd_df.loc[(pd_df['value'].apply(
                lambda x: isinstance(x, str) and (True if search_pattern.fullmatch(x) else False))), ['key', 'value']]

        else:
            search_pattern = search_value
            matched_df = pd_df.loc[pd_df['value'] == search_pattern, ['key', 'value']]

        matched_elements = {flatten_tuple(v['key']): v['value'] for i, v in matched_df.iterrows()}
    return matched_elements
