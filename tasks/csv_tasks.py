import json
import logging
import sys
import traceback
from collections import defaultdict

from aviso.settings import gnana_storage, sec_context
from pymongo.errors import BulkWriteError

from domainmodel.csv_data import CSVDataClass, csv_version_decorator

logger = logging.getLogger('gnana.%s' % __name__)


def csv_upload_task(kwargs):
    filepath = kwargs['filepath']
    csv_file = gnana_storage.open(filepath)
    reader = json.loads(csv_file.readlines()[0])
    return csvdatauploadprocess(reader, **kwargs)


@csv_version_decorator
def csvdatauploadprocess(reader,  **kwargs):
    filepath = kwargs.get('filepath',False)
    csv_type = kwargs['csv_type']
    csv_name = kwargs['csv_name']
    CSVClass = CSVDataClass(csv_type, csv_name, used_for_write=True)
    dd_type = kwargs['dd_type']
    static_type = kwargs['static_type']
    ret = {'inserts': 0, 'updates': 0}

    from_scratch = not(dd_type=='partial' or static_type=='partial')

    def process_batch(record_map):
        bulk_list = CSVClass.bulk_ops()
        starting = len(record_map)
        inserts = 0
        updates = 0
        criteria_list = record_map.keys()
        rec_list = []
        logger.info('-=' * 20)
            # csv_record = CSVClass.getByFieldValue('extid', unique_value)
        if not from_scratch and criteria_list:
            matched_csv_records = CSVClass.getAll(
                {'object.extid': {'$in': [sec_context.encrypt(x, CSVClass) if CSVClass.encrypted else x for x in criteria_list]}})
        else:
            logger.info('from_scratch mode enabled. Not distinguishing between insert and update.')
            matched_csv_records = []
        update_map = defaultdict(list)
        matched_keys = set()
        matched_count = 0
        for matched_rec in matched_csv_records:
            matched_count += 1
            try:
                matched_rec_extid = str(matched_rec.extid) if isinstance(matched_rec.extid, str) else matched_rec.extid
            except:
                matched_rec_extid = matched_rec.extid
            if matched_rec_extid in record_map:
                update_map[matched_rec_extid] = record_map.pop(
                    matched_rec_extid)
                matched_keys.add(matched_rec_extid)
            else:
                logger.info(
                    f'matched record is not found in the record map {matched_rec_extid}')
        logger.info(f'Matched records count {matched_count} - total criteria list {len(criteria_list)}')
        if not from_scratch:
            deals_to_insert = set(criteria_list) - matched_keys
        else:
            deals_to_insert = []
        for k in deals_to_insert:
            try:
                recs = record_map.pop(k) if k in record_map else None
                if recs:
                    csv_obj = CSVClass()
                    c_rec = recs.pop(0)
                    csv_obj.apply_fields(c_rec)
                    rec_list.append(csv_obj)
                else:
                    logger.info(
                        'Expecting record for insert - Key not found in the map of csv recs %s ' % k)
                if recs:
                    logger.info(
                        'Multiple records found for the same cache key %s - update count %s' % (k, len(recs)))
                    update_map[k] = recs
            except:
                logger.exception(f"Key not found {k}")
                pass
        # Insert recs
        try:
            CSVClass.bulk_insert(rec_list)
            inserts += len(rec_list)
        except Exception as e:
            logger.info(f'bulk_insert failed with msg  {e} \nsaving one_by_one')
            res = save_one_by_one(rec_list, **kwargs)
            updates = res['updates']
            inserts = res['inserts']
        rec_list = []
        # Update recs
        recs_to_update = record_map if from_scratch else update_map
        for key, csv_records in recs_to_update.iteritems():
            csv_obj = CSVClass() if from_scratch else CSVClass.getByFieldValue('extid', key)
            # FIXME: for some collections getByFieldValue returns a list of a single csv object??
            if isinstance(csv_obj, list):
                csv_obj = csv_obj[0]
            for csv_record in csv_records:
                updates += 1
                csv_obj.apply_fields(csv_record, partial_dd=(dd_type == 'partial'),
                                     partial_static=(static_type == 'partial'))
                csv_obj.save(bulk_list=bulk_list)
        if updates:
            try:
                bulk_list.execute()
            except BulkWriteError as e:
                logger.exception(e)
                e.message += "| " + str(e.details)[:1000] + " ... <truncated>"
                raise

        logger.info('Updated %s records ' % updates)
        logger.info('Inserted %s records ' % inserts)
        logger.info('starting %s ' % starting)
        ret['inserts'] += inserts
        ret['updates'] += updates
        return ret
    # checking the existance of table if the csv_type is a postgres enabled
    csv_obj = CSVClass()
    if csv_obj.postgres:
        prefix = "%s._csvdata.%s." % (sec_context.name, kwargs['csv_type'])
        all_collections = [x for x in CSVClass.list_all_collections(prefix)]
        if csv_obj.getCollectionName() not in all_collections:
            logger.info("Table not exist trying to create one.")
            try:
                tc = sec_context.details
                config = tc.get_config('csv_data', kwargs['csv_type'])
                csv_obj.create_postgres_table(config, tc.is_encrypted)
            except Exception as e:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                err_msg = "\n".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
                message = "\n".join(["ERROR: custom message", e, err_msg])
                logger.error(message)
                raise e

    batch_size = 5000
    rec_count = 0
    record_map = defaultdict(list)
    for record in reader:
        unique_value = CSVClass.getUniqueValue(record)
        record_map[unique_value].append(record)
        rec_count += 1
        if rec_count % batch_size == 0:
            process_batch(record_map)
            record_map = defaultdict(list)
    # process Remaining entries
    process_batch(record_map)
    if ret['inserts'] == 0 and ret['updates'] == 0:
        logger.info('Nothing passed to save in the CSV data upload')
        logger.info('-=' * 20)
    if filepath:
        gnana_storage.delete_adhoc_file(filepath)
        return ret
    return ret


def save_one_by_one(rec_list, **kwargs):
    csv_type = kwargs['csv_type']
    csv_name = kwargs['csv_name']
    CSVClass = CSVDataClass(csv_type, csv_name, used_for_write=True)
    dd_type = kwargs.get('dd_type', 'complete')
    static_type = kwargs.get('static_type', 'complete')
    res = {'inserts': 0, 'updates': 0}
    for record in rec_list:
        try:
            record.save()
            res['inserts'] += 1
        except:
            localattrs = {}
            record.encode(localattrs)
            localattrs.pop('extid')
            df = localattrs.pop('dynamic_fields')
            localattrs.update(df)
            csv_obj = CSVClass.getByFieldValue('extid', record.extid)
            if csv_obj:
                csv_obj.apply_fields(localattrs, partial_dd=(dd_type == 'partial'),
                                    partial_static=(static_type == 'partial'))
                res['updates'] += 1
            else:
                logger.info('unable to process %s' % record.extid)
            csv_obj.save()
    return res
