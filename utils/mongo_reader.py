import logging

import boto3
from aviso.settings import sec_context

from infra.constants import FORECAST_SCHEDULE_COLL

logger = logging.getLogger('gnana.%s' % __name__)


s3 = boto3.client('s3')
ONE_DAY = 24 * 60 * 60 * 1000
ONE_HOUR = 60 * 60 * 1000
MAX_THREADS = 200
SIX_HOURS = 6 * 60 * 60 * 1000

def get_forecast_schedule(nodes, db=None, get_dict={}):
    try:
        forecast_schedule_coll = db[FORECAST_SCHEDULE_COLL] if db else sec_context.tenant_db[FORECAST_SCHEDULE_COLL]
        criteria = {'node_id': {'$in': nodes}}
        projections = {"recurring": 1,
                       "unlockPeriod": 1,
                       "unlockFreq": 1,
                       "unlockDay": 1,
                       "unlocktime": 1,
                       "lockPeriod": 1,
                       "lockFreq": 1,
                       "lockDay": 1,
                       "locktime": 1,
                       "timeZone": 1,
                       "node_id": 1,
                       "lock_on": 1,
                       "unlock_on": 1,
                       "status": 1,
                       "non_recurring_timestamp": 1,
                       "status_non_recurring": 1
                       }
        response = forecast_schedule_coll.find(criteria, projections)
        if get_dict:
            return {res['node_id']: res for res in response}
        return [res for res in response]
    except Exception as ex:
        logger.exception(ex)
        return {'success': False, 'Error': str(ex)}


