import hashlib
import json

from aviso.settings import sec_context


def _determine_period_field_name(close_periods):
    """
    Determines the MongoDB field name for filtering based on the type of periods provided.

    Args:
        close_periods (list): List of periods to analyze (e.g., weekly or non-weekly).

    Returns:
        str: The field name ('weekly_period' if all periods contain 'W', else 'close_period').

    Raises:
        TypeError: If periods is not a list of strings.
        ValueError: If periods is empty.
    """
    if not isinstance(close_periods, list):
        raise TypeError("close_periods must be a list")
    if not close_periods:
        raise ValueError("close_periods list cannot be empty")
    if not all(isinstance(period, str) for period in close_periods):
        raise TypeError("all elements in close_periods must be strings")

    return 'weekly_period' if all('W' in period for period in close_periods) else 'close_period'

def create_collection_checksum(collection_name,
                               db=None,
                               ):
    """
    make a checksum for db collection

    Arguments:
        collection_name {str} -- name of collection

    Keyword Arguments:
        db {pymongo.database.Database} -- instance of tenant_db
                                    (default: {None})
                                    if None, will create one

    Returns:
        str -- checksum
    """
    db = db if db else sec_context.tenant_db
    collection = db[collection_name]
    recs = list(collection.find({}, {'_id': 0}))
    return hashlib.sha1(json.dumps(recs).encode('utf-8')).hexdigest()

