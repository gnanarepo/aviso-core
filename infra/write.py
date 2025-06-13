import logging

from aviso.settings import sec_context
from pymongo import UpdateOne

from infra.constants import FORECAST_SCHEDULE_COLL, DEALS_COLL
from utils.mongo_writer import bulk_write

logger = logging.getLogger('gnana.%s' % __name__)


def upload_forecast_schedule(nodes, records, db=None):
    try:
        forecast_schedule_coll = db[FORECAST_SCHEDULE_COLL] if db else sec_context.tenant_db[FORECAST_SCHEDULE_COLL]
        updates = []
        for rec in records:
            updates.append(UpdateOne({'node_id': rec['node_id']}, {'$set': rec}, upsert=True))
        bulk_write(updates, forecast_schedule_coll)
        logger.info("[FORECAST_SCHEDULE_COLL] : Updated records for node {} sample {}".format(nodes, records[0]))
        return {'success': True}
    except Exception as ex:
        logger.exception(ex)
        return {'success': False, 'Error': str(ex)}

def _remove_run_full_mode_flag():
    try:
        sec_context.details.remove_flag(DEALS_COLL, 'run_full_mode')
    except:
        pass
