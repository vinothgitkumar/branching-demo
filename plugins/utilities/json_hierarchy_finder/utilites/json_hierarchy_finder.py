import json

from plugins.utilities.json_hierarchy_finder.interface.json_hierarchy_finder import find_element


def find_hierarchy(input_json, search_value=None, search_key=None, occurrence=-1, regex=False):
    """
    Helps to search a specific or a regular expression value or key in a given json data.

    :param input_json: The JSON data passed by user.
    :param search_value: string/regex expression of the value that needs to be search within the values of the dictionary.
    :param search_key: string or regex expression of the key that needs to be search within the keys of the dictionary.
    :param occurrence: The nth occurrence times the value appears. By default, -1 which will return all the occurrences.
    :param regex: Set to true , it we need to find the results on regular expression provided.
    :return: The dict of hierarchy within json or None
    """
    if input_json is None:
        raise Exception("Please provide input")
    else:
        json.loads(str(input_json))

    if (search_key is not None and search_value is not None) or (search_value is None and search_key is None):
        raise Exception("Please provide either search_key or search_value")

    if occurrence < -1 or occurrence == 0:
        raise Exception("Please provide occurrence greater than 0")
    result = find_element(input_json=input_json, search_key=search_key, search_value=search_value, regex=regex)
    if result:
        if occurrence == -1:
            return result
        else:
            if occurrence > len(result):
                print(
                    "Passed occurrence value is more than the number of occurrences of the searching element in the provided JSON.")
                return None
            else:
                return {list(result.keys())[occurrence - 1]: result[list(result.keys())[occurrence - 1]]}
    else:
        return None
