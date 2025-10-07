import re
import pytz
import logging
from time import time
from json import loads, dumps
from datetime import datetime, timedelta
from dateutil.tz import gettz
from pymongo import MongoClient
from utils.data_load_utils import get_drilldowns, get_dd_list
from data_load.tenants import ms_connection_strings, get_bastion_server, get_postgres_connection,get_s3_connection,get_static_postgress_data
from base64 import b64decode
from zlib import decompress
from bson import BSON

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if not logger.handlers:
    ch = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

Debug = True

class DealsResultsService(object):
    def __init__(self, tenant_name, stack, gbm_stack, pod, etl_stack):
        self.tenant_name = tenant_name
        self.stack = stack
        self.gbm_stack = gbm_stack
        self.pod = pod
        self.etl_stack = etl_stack
        # Configs & setup
        (self.viewgen_config,
         self.uipfield,
         self.win_score_config) = get_configs(tenant_name, gbm_stack, stack) if not Debug else get_static_configs()

        self.drilldowns = get_drilldowns(tenant_name, stack, self.viewgen_config)

        ms_connection_string = ms_connection_strings(pod)
        logger.info('Connecting to MongoDB for %s %s', tenant_name, pod)
        self.client = MongoClient(ms_connection_string)
        self.db = self.client[tenant_name.split('.')[0] + '_db_' + etl_stack]

    def close(self):
        if self.client:
            self.client.close()

    def get_current_period_and_timezone(self):
        coll = self.db[self.tenant_name + '.Period._uip._data']
        records = list(coll.find({}, {'_id': 0}))
        timezone = 'US/Pacific'  # default timezone
        for record in records:
            if bool(record['object']['values'].get('TimeZoneSidKey')):
                timezone = loads(record['object']['values']['TimeZoneSidKey'])
                break
        current_date = datetime.now().astimezone(pytz.timezone(timezone)).strftime('%Y%m%d')
        curr_period = None

        for record in records:
            if bool(record['object']['values'].get('Type')):
                if loads(record['object']['values'].get('Type')).lower() == 'quarter':
                    start_date = loads(record['object']['values']['StartDate'])
                    end_date = loads(record['object']['values']['EndDate'])
                    if start_date <= current_date <= end_date:
                        label = loads(record['object']['values']['FullyQualifiedLabel'])
                        qtr_number = loads(record['object']['values']['Number'])
                        curr_period = label[-4:] + 'Q' + qtr_number
                        break

        return curr_period, timezone

    def compute_results(self, period, timestamp, get_results_from_as_of, fields, node, force_uip_and_hierarchy):
        curr_period, timezone = self.get_current_period_and_timezone()

        if period != curr_period and get_results_from_as_of:
            return {'message': 'get_results_from_as_of is supported only for current period'}
        db = self.client[self.tenant_name.split('.')[0] + '_db_' + self.gbm_stack]
        coll = db[self.tenant_name + '.OppDS._runs.existingpipeforecast._run']
        runs = list(coll.find({'object.as_of_mnemonic': period}, {'_id': 0}))
        latest_run = {'as_of': datetime(1900, 1, 1)}
        dt_timestamp = datetime.fromtimestamp(timestamp/1000.0) if timestamp else None

        for run in runs:
            if run['object']['run_time_horizon'].startswith('custom_Reporting') and run['object']['run_time_horizon'].endswith('235959'):
                if dt_timestamp:
                    if latest_run['as_of'] < run['object']['as_of'] < dt_timestamp:
                        latest_run = {'name': run['object']['run_time_horizon'],
                                      'as_of': run['object']['as_of']}
                else:
                    if latest_run['as_of'] < run['object']['as_of']:
                        latest_run = {'name': run['object']['run_time_horizon'],
                                      'as_of': run['object']['as_of']}

        latest_run = latest_run if latest_run.get('name') else {}

        live_run = {}
        if period == curr_period:
            coll = db[self.tenant_name + '.OppDS._runs.bookings_rtfm._run']
            runs = list(coll.find({'object.as_of_mnemonic': period}, {'_id': 0}))
            if runs:
                live_run = {'name': runs[0]['object']['run_time_horizon'],
                            'as_of': runs[0]['object']['as_of'],
                            'last_modified_time': runs[0]['last_modified_time']}

        s3_client = get_s3_connection()
        read_from_s3 = False
        first_key = timestamp if timestamp else period
        records = []
        result_ck = None
        result_timestamp = None
        if live_run:
            if latest_run:
                if (live_run['as_of'] + timedelta(hours=3) > latest_run['as_of']) and \
                        (not dt_timestamp or live_run['as_of'] < dt_timestamp):
                    result_ck = live_run['name'][7:]
                    result_timestamp = live_run['last_modified_time']
                    criteria = {'object.run_time_horizon': live_run['name']}
                    if get_results_from_as_of:
                        criteria = {'$and': [criteria, {'last_modified_time': {'$gte': get_results_from_as_of}}]}
                    coll = db[self.tenant_name + '.OppDS._results.bookings_rtfm._result']
                    records = list(coll.find(criteria, {'_id': 0}))
                else:
                    result_ck = latest_run['name'][7:]
                    file_name = '/'.join([self.tenant_name, self.gbm_stack, result_ck]) + '.csv'
                    try:
                        if force_uip_and_hierarchy:
                            s3_client.delete_object(Bucket='daily-results', Key=file_name)
                            raise Exception("Force refresh from database")
                        resp = {first_key : loads(s3_client.get_object(Bucket='daily-results', Key=file_name).get('Body').read())}
                        read_from_s3 = True
                    except:
                        result_timestamp = int(datetime.strptime(result_ck[25: 33], '%Y%m%d').replace(tzinfo=gettz(timezone)).timestamp() * 1000)
                        coll = db[self.tenant_name + '.OppDS._results.existingpipeforecast._result.' + period]
                        record = list(coll.find({'object.run_time_horizon': latest_run['name']}, {'_id': 0}))[0]
                        records = BSON(decompress(b64decode(record['_encdata']))).decode().get('records', [])
        elif latest_run:
            result_ck = latest_run['name'][7:]
            file_name = '/'.join([self.tenant_name, self.gbm_stack, result_ck]) + '.csv'
            try:
                if force_uip_and_hierarchy:
                    s3_client.delete_object(Bucket='daily-results', Key=file_name)
                    raise Exception("Force refresh from database")
                resp = {first_key : loads(s3_client.get_object(Bucket='daily-results', Key=file_name).get('Body').read())}
                read_from_s3 = False
            except:
                result_timestamp = int(datetime.strptime(result_ck[25: 33], '%Y%m%d').replace(tzinfo=gettz(timezone)).timestamp() * 1000)
                coll = db[self.tenant_name + '.OppDS._results.existingpipeforecast._result.' + period]
                record = list(coll.find({'object.run_time_horizon': latest_run['name']}, {'_id': 0}))[0]
                records = BSON(decompress(b64decode(record['_encdata']))).decode().get('records', [])
        else:
            return {'message': 'No runs found'}

        uipfields = {}
        requested_fields = None
        requested_node = None

        if not result_ck.startswith('live') and not read_from_s3:
            requested_fields = list(fields) if fields else None
            fields = []
            requested_node = str(node) if node else None
            node = None

        if result_ck.startswith('live') or not read_from_s3:
            if fields:
                for field in self.win_score_config.get('required_fields', []):
                    if field not in fields:
                        fields.append(field)
                for field in fields:
                    if field in self.uipfield:
                        uipfields[field] = field[6:] if field.startswith('as_of_') and not field.startswith('as_of_fv_') else field
            else:
                for field in self.uipfield:
                    uipfields[field] = field[6:] if field.startswith('as_of_') and not field.startswith('as_of_fv_') else field

            resp = {first_key: {'is_curr_q': period == curr_period,
                                'timestamp': result_timestamp,
                                'ck': result_ck,
                                'results': {}}}

            dict_flds = ['eACV', 'won_amount', 'lost_amount', 'active_amount', 'forecast',
                         'existing_pipe_active_amount', 'new_pipe_active_amount',
                         'existing_pipe_lost_amount', 'new_pipe_lost_amount',
                         'existing_pipe_won_amount', 'new_pipe_won_amount']

            for record in records:
                drilldowns_list, split_fields = get_dd_list(self.viewgen_config, record['object']['uipfield'], self.drilldowns, False)
                if node and node not in drilldowns_list:
                    continue

                resp[first_key]['results'][record['object']['extid']] = {'__segs': drilldowns_list}

                if fields:
                    for field in fields:
                        if field in uipfields:
                            if field in split_fields:
                                resp[first_key]['results'][record['object']['extid']][field] = split_fields[field]
                            else:
                                resp[first_key]['results'][record['object']['extid']][field] = record['object']['uipfield'].get(uipfields[field], 'N/A')
                        else:
                            val = record['object']['results'].get(field, 'N/A')
                            if node or field not in dict_flds:
                                resp[first_key]['results'][record['object']['extid']][field] = val
                            else:
                                resp[first_key]['results'][record['object']['extid']][field] = {drilldown: val for drilldown in drilldowns_list}
                else:
                    for field in self.uipfield:
                        resp[first_key]['results'][record['object']['extid']][field] = record['object']['uipfield'].get(uipfields[field], 'N/A')
                    for field, val in record['object']['results'].items():
                        if node or field not in dict_flds:
                            resp[first_key]['results'][record['object']['extid']][field] = val
                        else:
                            resp[first_key]['results'][record['object']['extid']][field] = {drilldown: val for drilldown in drilldowns_list}

            resp[first_key]['results'] = self.transform_score(resp[first_key]['results'], self.win_score_config, result_timestamp)

            if get_results_from_as_of:
                dd_criteria = {'$and': [{'object.run_time_horizon': 'ZZZ_DELETED'},
                                        {'last_modified_time': {'$gte': get_results_from_as_of}}]}
                dd_results = list(coll.find(dd_criteria, {'_id': 0}))
                for record in dd_results:
                    if record['object']['extid'] not in resp[first_key]['results']:
                        resp[first_key]['results'][record['object']['extid']] = {'is_deleted': True}

        if not result_ck.startswith('live'):
            # if not read_from_s3:
            #     s3_client.put_object(Bucket='daily-results', Key=file_name, Body=dumps(resp[first_key]))
            #     fields = list(requested_fields) if requested_fields else None
            #     node = str(requested_node) if requested_node else None

            if fields or node:
                new_results = {}
                for deal in resp[first_key]['results']:
                    if fields and node:
                        if node in resp[first_key]['results'][deal].get('__segs'):
                            new_results[deal] = {}
                            for fld in fields:
                                if fld in resp[first_key]['results'][deal]:
                                    val = resp[first_key]['results'][deal][fld]
                                    if type(val) == dict:
                                        new_results[deal][fld] = val.get(node, val)
                                    else:
                                        new_results[deal][fld] = val
                    elif fields:
                        new_results[deal] = {}
                        for fld in fields:
                            if fld in resp[first_key]['results'][deal]:
                                new_results[deal][fld] = resp[first_key]['results'][deal][fld]
                    else:
                        if node in resp[first_key]['results'][deal].get('__segs'):
                            new_results[deal] = {}
                            for fld, val in resp[first_key]['results'][deal].items():
                                if type(val) == dict:
                                    new_results[deal][fld] = val.get(node, val)
                                else:
                                    new_results[deal][fld] = val
                resp[first_key]['results'] = new_results

        return resp

    def transform_score(self, deal_results, win_score_config, asof=None):
        """Transform win scores according to configuration"""
        if not win_score_config.get('scale_win_probs') and not win_score_config.get('transform_lambda'):
            return deal_results

        nt = None
        if win_score_config.get('scale_win_probs'):
            threshold_config = win_score_config.get('threshold_multiplier', {
                '0.3': 2,
                '0.4': 1.9,
                '0.5': 1.5,
                '0.6': 1.25
            })
            threshold = win_score_config.get('win_prob_threshold', None)

            # TO DO: Save threshold in config directly
            if not threshold:
                for deal, dtls in deal_results.items():
                    if dtls.get('win_prob_threshold'):
                        threshold = dtls.get('win_prob_threshold')
                        break

            if threshold:
                if threshold <= 0.3:
                    nt = threshold * float(threshold_config['0.3'])
                elif 0.3 < threshold <= 0.4:
                    nt = threshold * float(threshold_config['0.4'])
                elif 0.4 < threshold <= 0.5:
                    nt = threshold * float(threshold_config['0.5'])
                elif 0.5 < threshold <= 0.6:
                    nt = threshold * float(threshold_config['0.6'])
                else:
                    nt = threshold

                ll = min(win_score_config.get('win_probs_ll', 0.001), threshold, nt)
                ul = max(win_score_config.get('win_probs_ul', 0.999), threshold, nt)

        win_score_low = win_score_config.get('win_score_low', 0.02)
        win_score_high = win_score_config.get('win_score_high', 0.98)

        for deal, dtls in deal_results.items():
            if dtls.get('is_deleted') or 'win_prob' not in dtls:
                continue

            try:
                dtls['orig_win_prob'] = float(dtls['win_prob'])
            except:
                dtls['orig_win_prob'] = 'N/A'

            if nt:
                dtls['win_prob_threshold'] = nt

            if dtls['orig_win_prob'] in [0.0, 1.0]:
                continue

            if dtls['win_prob'] != 'N/A':
                if threshold and nt != threshold:
                    win_score = dtls['win_prob']
                    if (win_score > ll and win_score < threshold):
                        dtls['win_prob'] = (((win_score - ll) * (nt - ll)) / (threshold - ll)) + ll
                    elif (win_score <= ul and win_score >= threshold):
                        dtls['win_prob'] = (((win_score - threshold) * (ul - nt)) / (ul - threshold)) + nt

                if win_score_config.get('transform_lambda') and asof:
                    transform_fn = eval(win_score_config.get('transform_lambda'))
                    dtls['win_prob'] = transform_fn(dtls, asof)

                dtls['win_prob'] = (dtls['win_prob'] * (win_score_high - win_score_low)) + win_score_low

            if dtls.get('winscoreprojections'):
                lower_dicts = dtls.get('winscoreprojections', {}).get('lower', [])
                upper_dicts = dtls.get('winscoreprojections', {}).get('upper', [])
                all_ws = [proj_dict['winscore'] for proj_dict in upper_dicts + lower_dicts]
                all_ws_dict = {}

                for ws in all_ws:
                    all_ws_dict[ws] = float(ws)
                    if threshold and nt != threshold:
                        win_score = all_ws_dict[ws]
                        if (win_score > ll and win_score < threshold):
                            all_ws_dict[ws] = (((win_score - ll) * (nt - ll)) / (threshold - ll)) + ll
                        elif (win_score <= ul and win_score >= threshold):
                            all_ws_dict[ws] = (((win_score - threshold) * (ul - nt)) / (ul - threshold)) + nt
                    all_ws_dict[ws] = (all_ws_dict[ws] * (win_score_high - win_score_low)) + win_score_low

                lower_inflated_dicts = []
                upper_inflated_dicts = []

                for proj_dict in upper_dicts:
                    if proj_dict['stagetrans_next'] not in ['-1', '99']:
                        proj_dict['winscore'] = str(all_ws_dict[proj_dict['winscore']])
                    upper_inflated_dicts.append(proj_dict)

                for proj_dict in lower_dicts:
                    if proj_dict['stagetrans_next'] not in ['-1', '99']:
                        proj_dict['winscore'] = str(all_ws_dict[proj_dict['winscore']])
                    lower_inflated_dicts.append(proj_dict)

                dtls['winscoreprojections'] = {
                    'lower': lower_inflated_dicts,
                    'upper': upper_inflated_dicts
                }

        return deal_results

def get_configs(self, tenant_name, gbm_stack, stack):
    """Get configurations from database"""
    logger.info('Connecting to bastion server for %s %s stack', tenant_name, stack)
    server = get_bastion_server(stack)
    conn = get_postgres_connection()
    cursor = conn.cursor()

    query = f"SELECT value FROM tenant_configs WHERE category IN ('datasets') AND config_name in ('OppDS') AND tenant_id IN (SELECT _id FROM tenant WHERE name='{tenant_name}');"
    cursor.execute(query)
    data = cursor.fetchall()
    data = loads(data[0][0])

    for element in data['models']:
        if element['name'] == 'common':
            viewgen_config = element['config']['viewgen_config']
            win_score_config = element['config'].get('win_score_config', {})

    uipfield = data['params']['general']['uipfield']
    conn.close()
    server.stop()
    return viewgen_config, uipfield, win_score_config




# Wrapper function
def get_deals_results(tenant_name, stack, gbm_stack, etl_stack, pod,
                      period, timestamp, get_results_from_as_of, fields, node, force_uip_and_hierarchy):
    service = DealsResultsService(tenant_name, stack, gbm_stack, pod, etl_stack)
    return( service.compute_results(
            period=period,
            timestamp=timestamp,
            get_results_from_as_of=get_results_from_as_of,
            fields=fields,
            node=node,
            force_uip_and_hierarchy=force_uip_and_hierarchy
        ))
    service.close()

def get_static_configs():
    data =  get_static_postgress_data()
    common_model = data['models']['common']

    # Extract the requested fields using .get() for safety
    viewgen_config = common_model['config']['viewgen_config']
    uipfield = data['params']['general']['uipfield']
    win_score_config=common_model['config'].get('win_score_config',{})

    return viewgen_config, uipfield,win_score_config