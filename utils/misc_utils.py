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