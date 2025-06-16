import hashlib
import json

from aviso.settings import sec_context

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

