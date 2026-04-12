import logging, json

import boto3
from aviso.settings import sec_context

# from infra.constants import FORECAST_SCHEDULE_COLL

logger = logging.getLogger('gnana.%s' % __name__)


s3 = boto3.client('s3')
ONE_DAY = 24 * 60 * 60 * 1000
ONE_HOUR = 60 * 60 * 1000
MAX_THREADS = 200
SIX_HOURS = 6 * 60 * 60 * 1000

# def get_forecast_schedule(nodes, db=None, get_dict={}):
#     try:
#         forecast_schedule_coll = db[FORECAST_SCHEDULE_COLL] if db else sec_context.tenant_db[FORECAST_SCHEDULE_COLL]
#         criteria = {'node_id': {'$in': nodes}}
#         projections = {"recurring": 1,
#                        "unlockPeriod": 1,
#                        "unlockFreq": 1,
#                        "unlockDay": 1,
#                        "unlocktime": 1,
#                        "lockPeriod": 1,
#                        "lockFreq": 1,
#                        "lockDay": 1,
#                        "locktime": 1,
#                        "timeZone": 1,
#                        "node_id": 1,
#                        "lock_on": 1,
#                        "unlock_on": 1,
#                        "status": 1,
#                        "non_recurring_timestamp": 1,
#                        "status_non_recurring": 1
#                        }
#         response = forecast_schedule_coll.find(criteria, projections)
#         if get_dict:
#             return {res['node_id']: res for res in response}
#         return [res for res in response]
#     except Exception as ex:
#         logger.exception(ex)
#         return {'success': False, 'Error': str(ex)}


def build_mongo_filter(record_filter):

    query_parts = []

    for expr, values in record_filter.items():
        if expr.endswith(')'):
            filter_type, field = expr[:-1].split('(')
        else:
            filter_type, field = 'in', expr

        field = field.strip()

        # Normalize values like Python loads() comparison
        norm_values = [json.dumps(v) if not (v.startswith('"') and v.endswith('"')) else v for v in values]

        # ID filters
        if filter_type == 'exclude_ids':
            query_parts.append({"_id": {"$nin": values}})
            continue

        if filter_type == 'include_ids':
            query_parts.append({"_id": {"$in": values}})
            continue

        mongo_field = f"object.values.{field}"

        # IN → must exist
        if filter_type == 'in':
            query_parts.append({
                "$and": [
                    {mongo_field: {"$exists": True}},
                    {mongo_field: {"$in": norm_values}}
                ]
            })

        # NOT IN → allow missing
        elif filter_type == 'not_in':
            query_parts.append({
                mongo_field: {"$nin": norm_values}
            })

        #MATCHES → must exist + all patterns match
        elif filter_type == 'matches':
            for pattern in values:
                query_parts.append({
                    "$and": [
                        {mongo_field: {"$exists": True}},
                        {mongo_field: {"$regex": f"^{pattern}"}}
                    ]
                })

        # NOT_MATCHES (same as Python bug)
        elif filter_type == 'not_matches':
            for pattern in values:
                query_parts.append({
                    "$and": [
                        {mongo_field: {"$exists": True}},
                        {mongo_field: {"$regex": f"^{pattern}"}}
                    ]
                })

    return {"$and": query_parts}

