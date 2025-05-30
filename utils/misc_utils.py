def get_nested(input_dict, nesting, default=None):
    """
    get value (or dict) from nested dict by specifying the levels to go through
    """
    if not nesting:
        return input_dict
    try:

        for level in nesting[:-1]:
            input_dict = input_dict.get(level, {})

        return input_dict.get(nesting[-1], default)
    except AttributeError:
        # Not a completely nested dictionary
        return default


def is_lead_service(service):
    if service and service=='leads':
        return True
    return False


def merge_dicts(*dictionaries):
    """
    combine dictionaries together
    """
    result = {}
    for dictionary in dictionaries:
        result.update(dictionary)
    return result

def iter_chunks(my_list, batch_size=5):
    start = 0
    more = True
    while more:
        end = start + batch_size
        if end < len(my_list):
            yield my_list[start:end]
            start = end
        else:
            more = False
            yield my_list[start:]
