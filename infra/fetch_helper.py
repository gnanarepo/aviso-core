
def modify_hint_field(close_periods, hint_field):
    """
    Modify MongoDB hint field based on close periods.
    """
    if all(['W' in close_period for close_period in close_periods]) and hint_field:
        hint_field = hint_field.replace('close_period', 'weekly_period')
    return hint_field
