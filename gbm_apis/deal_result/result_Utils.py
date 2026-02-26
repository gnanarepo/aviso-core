from aviso.utils.dateUtils import datetime2epoch, current_period,epoch2datetime,\
epoch, xl2datetime, get_prevq_mnem
from aviso.utils.misc_utils import try_float
from django.http.response import (HttpResponseNotFound)
from django.http.request import QueryDict
import traceback
import sys
from aviso.settings import sec_context,gnana_db
import logging
from aviso.utils import GnanaError
from copy import deepcopy
from aviso.settings import gnana_storage, CNAME
import datetime
from aviso.utils import dateUtils
from ..deal_result.dataset import get_time_horizon, Dataset, get_result_class
from ..deal_result.viewgen_service import CoolerViewGeneratorService
from ..deal_result.splitter_service import ViewGeneratorService
from ..deal_result.node_service import gimme_node_service
from ..deal_result.hierarchy_service import CoolerHierarchyService

logger = logging.getLogger('gnana.%s' % __name__)
basestring = str
long = int


def unicode_conversion(text):
    if isinstance(text, bytes):
        return str(text, 'utf-8')
    else:
        return text

unicode = unicode_conversion

config_restrict_paths = ['datasets.<>.maps', 'datasets.<>.filetypes']




def add_prefix(aFeature, default_prefix='latest_'):
    # Eswar claims this optimization gives a 5% performance improvement in model runtime.
    # Be careful to choose prefixes that don't invalidate this optimization.
    if aFeature[0] == 'a':
        if(aFeature[:9] == 'as_of_fv_' or
                aFeature[:11] == 'atbegin_fv_' or
                aFeature[:13] == 'athorizon_fv_' or
                aFeature[:6] == 'as_of_' or
                aFeature[:8] == 'atbegin_' or
                aFeature[:10] == 'athorizon_'):
            return aFeature
    else:
        if aFeature[:7] in ['latest_', 'frozen_']:
            return aFeature
    return default_prefix + aFeature


def try_float(maybe_float, default=None):
    try:
        return float(maybe_float)
    except:
        return default

def transform_score(deal_results, win_score_config, asof=None):
    '''
        This function takes deal results with win scores and transforms the scores based on the threshold value
        If the win score of a deal is greater than the threshold then the win score is transformed within a
        scale between [ 50 - 100 ] on the other hand if the win score is lesser than the threshold then the score
        is transformed within a scale of [ 0 - 50 ]

        Parameters:
            alpha --> Max limit of the transformed score for deals which are having greater scores than threshold
            beta ---> Min limit of the transformed score for deals which are having greater scores than threshold /
                      Max limit of the transformed score for deals which are having lesser scores than threshold
            theta --> Min limit of the transformed score for deals which are having lesser scores than threshold
        Input:
            Dictionary with deals along with it's attributes
            deal_result['0061T00000r2krN'] : {
                            u'AccountClassification': u'Named',
                            u'AccountSubClassification': u'Strategic',
                            ....
                            ....
                            u'terminal_fate': u'N/A',
                            u'win_prob': 0.30,
                            u'win_prob_threshold': 0.20
        Output:
            deal_result['0061T00000r2krN'] : {
                            u'AccountClassification': u'Named',
                            u'AccountSubClassification': u'Strategic',
                            ....
                            ....
                            u'terminal_fate': u'N/A',
                            u'win_prob': 0.70,
                            u'win_prob_threshold': 0.71
                            }
    '''

    if not win_score_config.get('scale_win_probs') and not win_score_config.get('transform_lambda'):
        return deal_results

    nt = None
    if win_score_config.get('scale_win_probs'):
        threshold_config = win_score_config.get('threshold_multiplier', {
            '0.3' : 2,
            '0.4' : 1.9,
            '0.5' : 1.5,
            '0.6' : 1.25
        })

        threshold = win_score_config.get('win_prob_threshold', None)

        # TO DO : Save threshold in config directly
        if not threshold:
            for deal,dtls in deal_results.items():
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
                    dtls['win_prob'] = ( ( ( win_score - ll ) * ( nt - ll ) ) / ( threshold - ll ) ) + ll
                elif (win_score <= ul and win_score >= threshold):
                    dtls['win_prob'] = ( ( ( win_score - threshold ) * ( ul - nt ) ) / (ul - threshold )) + nt
            if win_score_config.get('transform_lambda') and asof:
                transform_fn = eval(win_score_config.get('transform_lambda'))
                dtls['win_prob'] = transform_fn(dtls, asof)
            dtls['win_prob'] = (dtls['win_prob'] * (win_score_high - win_score_low)) + win_score_low

        if dtls.get('winscoreprojections'):
            lower_dicts = dtls.get('winscoreprojections', {}).get('lower', [])
            upper_dicts = dtls.get('winscoreprojections', {}).get('upper', [])
            all_ws = [proj_dict['winscore'] for proj_dict in upper_dicts+lower_dicts]
            all_ws_dict = {}
            for ws in all_ws:
                all_ws_dict[ws] = float(ws)
                if threshold and nt != threshold:
                    win_score = all_ws_dict[ws]
                    if (win_score > ll and win_score < threshold):
                        all_ws_dict[ws] = ( ( ( win_score - ll ) * ( nt - ll ) ) / ( threshold - ll ) ) + ll
                    elif (win_score <= ul and win_score >= threshold):
                        all_ws_dict[ws] = ( ( ( win_score - threshold ) * ( ul - nt ) ) / (ul - threshold )) + nt
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




def generate_subscription_same_dd_single_rec(rev_period, drilldown, close_date_fld, filt_config, opp_id, res):
    '''
    single rec version of the generate_subscription_same_dd_recs function
    '''
    conditional_field = filt_config.get('conditional_field', 'as_of_TermType')
    conditional_field_val = filt_config.get('conditional_field_val', ['Churn', 'Renewal'])
    terminal_fate_fld = filt_config.get('terminal_fate_fld', 'as_of_terminal_fate')
    if terminal_fate_fld not in res:
        terminal_fate_fld = 'as_of_' + terminal_fate_fld
    required_amt_fld = filt_config.get('required_amt_fld', 'as_of_UpsellACV')
    ret_val = {}
    current_period_mne = current_period().mnemonic
    deal_rev_sched_amount = res.get('as_of_raw_rev_schedule_amounts', {})
    deal_rev_sched_date = res.get('as_of_raw_rev_schedule_dates', {})
    deal_close_date = res.get(close_date_fld, 'N/A')
    deal_conditional_field_val = res.get(conditional_field, 'N/A')

    # if close date is "N/A" then just return the records
    if deal_close_date == 'N/A':
        ret_val[opp_id] = dict(res)
        if deal_conditional_field_val in conditional_field_val:
            if not (ret_val[opp_id][terminal_fate_fld] == 'W' and ret_val[opp_id].get(required_amt_fld, 0.0) > 0.0):
                ret_val[opp_id]['__segs'] =  [seg for seg in ret_val[opp_id]['__segs'] if not seg.split("#")[0] in drilldown]

            for mne, amt in deal_rev_sched_amount.items():
                dummy_id = opp_id+'_'+mne
                ret_val[dummy_id] = dict(res)
                ret_val[dummy_id]['rev_schedule_amount'] = amt
                ret_val[dummy_id][close_date_fld] = deal_rev_sched_date.get(mne, 'N/A')
                ret_val[dummy_id]['__segs'] = [seg for seg in ret_val[dummy_id]['__segs'] if seg.split("#")[0] in drilldown]
                ret_val[dummy_id]['__dummy_deal_rec'] = True

        return ret_val

    deal_close_date_prd = current_period(a_datetime=xl2datetime(deal_close_date)).mnemonic
    # when rev_period is in past
    if rev_period < current_period_mne:
        ret_val[opp_id] = dict(res)
        ret_val[opp_id]['rev_schedule_amount'] = deal_rev_sched_amount.get(rev_period, 0.0)
        if deal_conditional_field_val in conditional_field_val:
            #do not remove configured drilldown if deal is open and deal has perticular amount
            if not (ret_val[opp_id][terminal_fate_fld] == 'W' and ret_val[opp_id].get(required_amt_fld, 0.0) > 0.0):
                ret_val[opp_id]['__segs'] =  [seg for seg in ret_val[opp_id]['__segs'] if not seg.split("#")[0] in drilldown]

            if rev_period in deal_rev_sched_amount:
                dummy_id = opp_id+'_'+rev_period
                ret_val[dummy_id] = dict(res)
                ret_val[dummy_id]['rev_schedule_amount'] = deal_rev_sched_amount.get(rev_period, 0.0)
                ret_val[dummy_id][close_date_fld] = deal_rev_sched_date.get(rev_period, 'N/A')
                ret_val[dummy_id]['__segs'] = [seg for seg in ret_val[dummy_id]['__segs'] if seg.split("#")[0] in drilldown]
                ret_val[dummy_id]['__dummy_deal_rec'] = True

    # when rev_period is current period
    else:
        # the deals with close date period in past would be ignored in caches run on forecast app end
        ret_val[opp_id] = dict(res)
        # for original deal record, revenue schedule amount should be taken from deal close date period
        ret_val[opp_id]['rev_schedule_amount'] = deal_rev_sched_amount.get(deal_close_date_prd, 0.0)
        if deal_conditional_field_val in conditional_field_val:
            #do not remove configured drilldown if deal is open and deal has perticular amount
            if not (ret_val[opp_id][terminal_fate_fld] == 'W' and ret_val[opp_id].get(required_amt_fld, 0.0) > 0.0):
                ret_val[opp_id]['__segs'] = [seg for seg in ret_val[opp_id]['__segs'] if not seg.split("#")[0] in drilldown]

            # create record for every remaining revenue period
            for mne, amt in deal_rev_sched_amount.items():
                if mne >= current_period_mne:
                    dummy_id = opp_id+'_'+mne
                    ret_val[dummy_id] = dict(res)
                    ret_val[dummy_id]['rev_schedule_amount'] = amt
                    ret_val[dummy_id][close_date_fld] = deal_rev_sched_date.get(mne, 'N/A')
                    ret_val[dummy_id]['__segs'] = [seg for seg in ret_val[dummy_id]['__segs'] if seg.split("#")[0] in drilldown]
                    ret_val[dummy_id]['__dummy_deal_rec'] = True

    return ret_val



def generate_expiration_date_renewal_rec(rev_period, renewal_drilldown, close_date_fld, expiration_date_fld, type_fld, renewal_vals, opp_id, res, splitted_fields):
    ret_val = {}
    dict_flds = ['eACV', 'won_amount', 'lost_amount', 'active_amount', 'forecast',
                 'existing_pipe_active_amount', 'existing_pipe_won_amount', 'existing_pipe_lost_amount',
                 'new_pipe_active_amount', 'new_pipe_won_amount', 'new_pipe_lost_amount'] + splitted_fields

    deal_type = res.get(type_fld, 'N/A')
    try:
        deal_close_date_prd = current_period(a_datetime=xl2datetime(res[close_date_fld])).mnemonic
    except:
        deal_close_date_prd = 'N/A'

    if deal_close_date_prd == 'N/A' or deal_close_date_prd >= rev_period:
        ret_val[opp_id] = deepcopy(res)
        ret_val[opp_id]['__segs'] = [element for element in ret_val[opp_id]['__segs'] if not element.startswith(renewal_drilldown+'#')]
        for fld in dict_flds:
            if fld in ret_val[opp_id]:
                for element in list(ret_val[opp_id][fld]):
                    if element.startswith(renewal_drilldown+'#'):
                        del(ret_val[opp_id][fld][element])
    if deal_type in renewal_vals:
        dummy_id = opp_id+'_renewal'
        ret_val[dummy_id] = deepcopy(res)
        ret_val[dummy_id]['__segs'] = [element for element in ret_val[dummy_id]['__segs'] if element.startswith(renewal_drilldown + '#')]
        for fld in dict_flds:
            if fld in ret_val[dummy_id]:
                for element in list(ret_val[dummy_id][fld]):
                    if not element.startswith(renewal_drilldown + '#'):
                        del(ret_val[dummy_id][fld][element])
        ret_val[dummy_id][close_date_fld] = res.get(expiration_date_fld, 0.0)
        ret_val[dummy_id]['__dummy_deal_rec'] = True

    return ret_val


def generate_expiration_date_renewal_rec_github(rev_period, drilldown, close_date_fld, expiration_date_fld, type_fld,
                                                renewal_vals, segment_config, rev_schedule_config, opp_id, res):
    ret_val = {}
    dict_flds = ['eACV', 'won_amount', 'lost_amount', 'active_amount', 'forecast',
                 'existing_pipe_active_amount', 'existing_pipe_won_amount', 'existing_pipe_lost_amount',
                 'new_pipe_active_amount', 'new_pipe_won_amount', 'new_pipe_lost_amount']

    deal_type = res.get(type_fld, 'N/A')

    try:
        deal_close_date_prd = current_period(a_datetime=xl2datetime(res[close_date_fld])).mnemonic
    except:
        deal_close_date_prd = 'N/A'

    if deal_close_date_prd == 'N/A' or deal_close_date_prd >= rev_period:
        ret_val[opp_id] = deepcopy(res)
        ret_val[opp_id]['__segs'] = [element for element in ret_val[opp_id]['__segs'] if
                                     not element.startswith(drilldown + '#')]
        for fld in dict_flds:
            if fld in ret_val[opp_id]:
                for element in list(ret_val[opp_id][fld]):
                    if element.startswith(drilldown + '#'):
                        del (ret_val[opp_id][fld][element])
    if deal_type in renewal_vals:
        dummy_id = opp_id + '_metered'
        ret_val[dummy_id] = deepcopy(res)
        ret_val[dummy_id]['__segs'] = [element for element in ret_val[dummy_id]['__segs'] if
                                       element.startswith(drilldown + '#')]
        for fld in dict_flds:
            if fld in ret_val[dummy_id]:
                for element in list(ret_val[dummy_id][fld]):
                    if not element.startswith(drilldown + '#'):
                        del (ret_val[dummy_id][fld][element])

        seg_amounts_dict = segment_config['amounts']
        total_amount_fld = segment_config['amount_fld']
        # rev_amounts_details = res.get(rev_schedule_config['product_list_field'], [])

        res2 = deepcopy(ret_val[dummy_id])
        try:
            ret_vals_dummy = generate_revenue_single_rec_github(rev_period, drilldown, close_date_fld,
                                                                dummy_id, res2, seg_amounts_dict,
                                                                total_amount_fld, rev_schedule_config)
            if dummy_id not in ret_vals_dummy:
                ret_val.pop(dummy_id, None)
        except:
            logger.warning('Github dealID:' + str(dummy_id) + '. Exception while computing dummy deals')
            ret_vals_dummy = {}
            ret_val.pop(dummy_id, '')

        ret_val.update(ret_vals_dummy)

    #         ret_val[dummy_id][close_date_fld] = res_.get(expiration_date_fld, 0.0)

    return ret_val


def generate_revenue_single_rec_github(rev_period, drilldown, close_date_fld, opp_id, res,
                                       seg_amounts_dict, total_amount_fld, rev_schedule_config):
    ret_val = {}

    rev_amounts_details = res.get(rev_schedule_config['product_list_field'], [])
    status_not_in = rev_schedule_config.get('Status_not_in', [])

    # Filtering out products base don Status condition
    rev_amounts_details = [ele for ele in rev_amounts_details if ele['Status'] not in status_not_in]

    # making all the segment amounts zero as we will update it below
    for k, v in seg_amounts_dict.items():
        res[v[0]] = 0

    current_period_mne = current_period().mnemonic
    #     deal_rev_sched_amount = res_.get('as_of_raw_rev_schedule_amounts', {})
    #     deal_rev_sched_date = res_.get('as_of_raw_rev_schedule_dates', {})

    # making amounts float if they are string
    for ele in rev_amounts_details:
        try:
            ele['Amount'] = float(ele['Amount'])
        except:
            ele['Amount'] = 0

    # adding quarter mnemonic to the products list
    for ele in rev_amounts_details:
        try:
            ele['qtr'] = current_period(a_datetime=xl2datetime(ele['Estimated Date'])).mnemonic
            # ele['qtr'] = src.current_period(src.epoch(ele['Estimated Date']).as_epoch())['mnemonic']
        except:
            ele['qtr'] = 'N/A'

    #####################################################
    deal_rev_sched_amount = {}
    for ele in rev_amounts_details:
        if ele['qtr'] not in deal_rev_sched_amount:
            deal_rev_sched_amount[ele['qtr']] = [ele]
        else:
            deal_rev_sched_amount[ele['qtr']].append(ele)

    deal_rev_sched_date = {}
    for ele in rev_amounts_details:
        prd = ele['qtr']
        if prd not in deal_rev_sched_date:
            deal_rev_sched_date[prd] = float(ele['Estimated Date'])
        else:
            deal_rev_sched_date[prd] = max(deal_rev_sched_date[prd], float(ele['Estimated Date']))


    deal_close_date = res.get(close_date_fld, 'N/A')
    if deal_close_date == "N/A":
        ret_val[opp_id] = dict(res)
        return ret_val

    deal_close_date_prd = current_period(a_datetime=xl2datetime(deal_close_date)).mnemonic
    # deal_close_date_prd = src.current_period(src.epoch(deal_close_date).as_epoch())['mnemonic']

    if rev_period < current_period_mne:
        logger.info('Github dealID:' + str(opp_id) + 'inside first if')
        revenue_segs = [seg for seg in res['__segs'] if
                        seg.startswith(drilldown + '#')]
        if deal_close_date_prd == rev_period:
            ret_val[opp_id] = dict(res)
            #         ret_val[opp_id]['rev_schedule_amount'] = deal_rev_sched_amount.get(rev_period, 0.0)
            ret_val[opp_id]['rev_schedule_amount'] = sum(
                [float(ele['Amount']) for ele in deal_rev_sched_amount.get(deal_close_date_prd, {})])
            ret_val[opp_id][total_amount_fld] = ret_val[opp_id]['rev_schedule_amount']
            amt_list = deal_rev_sched_amount.get(deal_close_date_prd, {})
            # Updating the product list so that table in UI shows correct qtr products
            ret_val[opp_id][rev_schedule_config['product_list_field']] = amt_list
            # updating segments amounts
            for product in amt_list:
                ret_val[opp_id][seg_amounts_dict.get(product['Segment'])[0]] += product['Amount']

            if 'won_amount' in ret_val[opp_id]:
                for seg in revenue_segs:
                    ret_val[opp_id]['won_amount'][seg] = ret_val[opp_id]['rev_schedule_amount']
        elif rev_period in deal_rev_sched_amount:
            dummy_id = opp_id + '_' + rev_period
            ret_val[dummy_id] = dict(res)
            ret_val[dummy_id]['rev_schedule_amount'] = sum(
                [float(ele['Amount']) for ele in deal_rev_sched_amount.get(deal_close_date_prd, {})])
            ret_val[dummy_id][total_amount_fld] = ret_val[dummy_id]['rev_schedule_amount']
            ret_val[dummy_id][close_date_fld] = deal_rev_sched_date.get(rev_period, 'N/A')
            # updating segments amounts
            amt_list = deal_rev_sched_amount.get(deal_close_date_prd, {})
            # Updating the product list so that table in UI shows correct qtr products
            ret_val[dummy_id][rev_schedule_config['product_list_field']] = amt_list
            for product in amt_list:
                ret_val[dummy_id][seg_amounts_dict.get(product['Segment'])[0]] += product['Amount']

            ret_val[dummy_id]['__segs'] = [seg for seg in ret_val[dummy_id]['__segs'] if
                                           seg.startswith(drilldown + '#')]
            ret_val[dummy_id]['won_amount'] = {seg: ret_val[dummy_id]['rev_schedule_amount'] for seg in revenue_segs}
        else:
            ret_val = {}
    else:
        logger.info('Github dealID:' + str(opp_id) + 'inside else')
        ret_val[opp_id] = dict(res)
        # for original deal record, revenue schedule amount should be taken from deal close date period
        #     ret_val[opp_id]['rev_schedule_amount'] = deal_rev_sched_amount.pop(deal_close_date_prd, 0.0)
        # TODO: Change this
        amt_list = deal_rev_sched_amount.get(deal_close_date_prd, [])
        ret_val[opp_id]['rev_schedule_amount'] = sum(
            [float(ele['Amount']) for ele in deal_rev_sched_amount.pop(deal_close_date_prd, {})])
        ret_val[opp_id][total_amount_fld] = ret_val[opp_id]['rev_schedule_amount']
        # Updating the product list so that table in UI shows correct qtr products
        ret_val[opp_id][rev_schedule_config['product_list_field']] = amt_list

        for product in amt_list:
            #             print ret_val[opp_id][seg_amounts_dict.get(product['Segment'])[0]], product['Amount'], product['Segment'], type(product['Amount'])
            ret_val[opp_id][seg_amounts_dict.get(product['Segment'])[0]] += product['Amount']

        revenue_segs = [seg for seg in ret_val[opp_id]['__segs'] if
                        seg.startswith(drilldown + '#')]
        if 'won_amount' in ret_val[
            opp_id]:  # For revenue segments won_amount will be revenue scheduled amount and not the actual amount
            for seg in revenue_segs:
                ret_val[opp_id]['won_amount'][seg] = ret_val[opp_id]['rev_schedule_amount']
        if deal_close_date_prd < current_period_mne:  # changing won_amount to 0 for past quarter deals
            if 'won_amount' in ret_val[opp_id] and type(ret_val[opp_id]['won_amount']) == dict:
                for seg in ret_val[opp_id]['__segs']:
                    ret_val[opp_id]['won_amount'][seg] = 0
            else:
                ret_val[opp_id]['won_amount'] = 0

        # Since there is no product for that qtr why create the dummy deal
        if not len(amt_list):
            ret_val.pop(opp_id, None)


        # create record for every remaining revenue period
        for mne, amt_list in deal_rev_sched_amount.items():
            #or condition is for case where current prd is close dt period but products are in next period
            if (mne >= current_period_mne and deal_close_date_prd != current_period_mne) or \
                    (mne > current_period_mne):  # we don't need to create dummy record if deal_close_date_prd = current_period_mne as revenue scheduling is handled above for that case
                dummy_id = opp_id + '_' + mne
                ret_val[dummy_id] = dict(res)
                ret_val[dummy_id]['rev_schedule_amount'] = sum([float(ele['Amount']) for ele in amt_list])
                ret_val[dummy_id][total_amount_fld] = ret_val[dummy_id]['rev_schedule_amount']
                ret_val[dummy_id][close_date_fld] = deal_rev_sched_date.get(mne, 'N/A')
                ret_val[dummy_id]['__segs'] = [seg for seg in ret_val[dummy_id]['__segs'] if
                                               seg.startswith(drilldown + '#')]
                ret_val[dummy_id][rev_schedule_config['product_list_field']] = amt_list

                logger.info('Github dealID:' + str(dummy_id) + 'mne:' + str(mne) +'current_period_mne:' + str(current_period_mne))
                for product in amt_list:
                    ret_val[dummy_id][seg_amounts_dict.get(product['Segment'])[0]] += product['Amount']
                if mne > current_period_mne:  # making won_amount as 0 for future quarter dummy deals
                    ret_val[dummy_id]['won_amount'] = {seg: 0 for seg in revenue_segs}
                else:
                    ret_val[dummy_id]['won_amount'] = {seg: ret_val[dummy_id]['rev_schedule_amount'] for seg in
                                                       revenue_segs}
    return ret_val


def generate_appannie_dummy_recs(rev_period, rev_schedule_config, opp_id, res):

    # logger.info('Appannie dealID:' + str(opp_id))

    ret_val = {}

    close_date_fld = rev_schedule_config.get('close_date_fld', 'as_of_CloseDate_adj')
    subscription_date_fld = rev_schedule_config.get('subscription_date_fld', 'as_of_SubscriptionDate')
    total_amount_fld = rev_schedule_config.get('total_amount_fld', 'as_of_Amount_USD')
    global_amount_fld = rev_schedule_config.get('global_amount_fld', 'as_of_Global_Amount_USD')
    renewal_amount_fld = rev_schedule_config.get('renewal_amount_fld', 'as_of_RenewalAmount_USD')
    upsell_amount_fld = rev_schedule_config.get('upsell_amount_fld', 'as_of_UpsellAmount_USD')
    total_amount_components = rev_schedule_config.get('total_amount_components',
                                                      ['as_of_UpsellAmount_USD', 'as_of_RenewalAmount_USD',
                                                       'as_of_NewBusiness_Amount_USD'])
    close_date_amounts = rev_schedule_config.get('close_date_amounts', {'as_of_Amount_USD': u'lambda x : True'})
    subscription_date_amounts = rev_schedule_config.get('subscription_date_amounts', {'as_of_Amount_USD': u'lambda x : True'})
    delayed_renewal_amounts = rev_schedule_config.get('delayed_renewal_amounts', {'as_of_Amount_USD': u'lambda x : True'})

    dict_flds = ['eACV', 'won_amount', 'lost_amount', 'active_amount', 'forecast',
                 u'existing_pipe_active_amount', u'new_pipe_active_amount',
                 u'existing_pipe_lost_amount', u'new_pipe_lost_amount',
                 u'existing_pipe_won_amount', u'new_pipe_won_amount']

    deal_close_date = res.get(close_date_fld, 'N/A')
    deal_subscription_date = res.get(subscription_date_fld, 'N/A')

    try:
        deal_close_date_prd = current_period(a_datetime=xl2datetime(deal_close_date)).mnemonic
    except:
        deal_close_date_prd = 'N/A'
    try:
        deal_subscription_date_prd = current_period(a_datetime=xl2datetime(deal_subscription_date)).mnemonic
    except:
        deal_subscription_date_prd = 'N/A'


    try:
        deal_close_date_month = current_period(a_datetime=xl2datetime(deal_close_date), period_type='M').mnemonic
    except:
        deal_close_date_month = 'N/A'
    try:
        deal_subscription_date_month = current_period(a_datetime=xl2datetime(deal_subscription_date), period_type='M').mnemonic
    except:
        deal_subscription_date_month = 'N/A'


    delayed_renewal_amount_list = [fld for fld, dtls in delayed_renewal_amounts.items() if res[fld] and eval(dtls)(res)]

    # Handling delayed renewals
    __delayed_renewal = False
    res['__delayed_renewal'] = __delayed_renewal
    if deal_close_date_prd == rev_period and deal_subscription_date_prd == get_prevq_mnem(rev_period):
        if res.get('as_of_StageTrans', '-1') not in {'-1', '99'}:
            ret_val[opp_id] = deepcopy(res)
            __delayed_renewal = True
            ret_val[opp_id]['__delayed_renewal'] = True
            # return  ret_val

    if not(res.get(renewal_amount_fld, 0) and res.get(upsell_amount_fld, 0)) and \
            deal_close_date_month == deal_subscription_date_month:
        ret_val[opp_id] = deepcopy(res)
        for fld in delayed_renewal_amount_list:
            ret_val[opp_id][fld] = 0.0
    else:
        close_date_amounts_list = [fld for fld,dtls in close_date_amounts.items() if res[fld] and eval(dtls)(res)]
        subscription_date_amount_list = [fld for fld,dtls in subscription_date_amounts.items() if res[fld] and eval(dtls)(res)]
        try:
            close_date_amounts = {fld: float(res[fld]) for fld in close_date_amounts_list}
        except:
            logger.info('Appannie dealID:' + str(opp_id)+ '. Exception while computing close_date_amounts')
            close_date_amounts = {fld: float(res[fld]) if res[fld] != 'N/A' else 0.0 for fld in close_date_amounts_list}
        try:
            subscription_date_amounts = {fld: float(res[fld]) for fld in subscription_date_amount_list}
        except:
            logger.info('Appannie dealID:' + str(opp_id) + '. Exception while computing subscription_date_amounts')
            subscription_date_amounts = {fld: float(res[fld]) if res[fld] != 'N/A' else 0.0 for fld in subscription_date_amount_list}
        close_date_amt_sum = sum([amt for fld,amt in close_date_amounts.items() if fld in total_amount_components])
        subscription_date_amt_sum = sum([amt for fld,amt in subscription_date_amounts.items() if fld in total_amount_components])
        total_amount = float(res[total_amount_fld])
        if deal_close_date_prd != 'N/A' and deal_close_date_prd >= rev_period:
            ret_val[opp_id] = deepcopy(res)
            if total_amount:
                ratio = close_date_amt_sum/total_amount
                for fld in dict_flds:
                    if fld in ret_val[opp_id]:
                        for node in ret_val[opp_id][fld]:
                            ret_val[opp_id][fld][node] *= ratio

            for fld in subscription_date_amount_list:
                ret_val[opp_id][fld] = 0.0

            for fld in delayed_renewal_amount_list:
                ret_val[opp_id][fld] = 0.0
            if __delayed_renewal:
                ret_val[opp_id]['__delayed_renewal'] = True
            ret_val[opp_id][total_amount_fld] = close_date_amt_sum

            if ret_val[opp_id].get(renewal_amount_fld, 0) != 0:
                ret_val[opp_id][global_amount_fld] = ret_val[opp_id][renewal_amount_fld]
            elif ret_val[opp_id].get(upsell_amount_fld) != 0:
                ret_val[opp_id][global_amount_fld] = ret_val[opp_id][upsell_amount_fld]

        if deal_subscription_date_prd != 'N/A' and deal_subscription_date_prd >= rev_period:
            dummy_id = opp_id+'_'+deal_subscription_date_prd
            ret_val[dummy_id] = deepcopy(res)
            for fld in delayed_renewal_amount_list:
                if opp_id in ret_val: # Quick Fix for the issue. Need to confirm Delayed Renewal Logic later.
                    ret_val[opp_id][fld] = 0.0
                # ret_val[dummy_id][fld] = 0.0 # This should be the line instead of above two lines. Need to confirm.
            ret_val[dummy_id][close_date_fld] = deal_subscription_date
            if total_amount:
                ratio = subscription_date_amt_sum/total_amount
                for fld in dict_flds:
                    if fld in ret_val[dummy_id]:
                        for node in ret_val[dummy_id][fld]:
                            ret_val[dummy_id][fld][node] *= ratio

            for fld in close_date_amounts_list:
                ret_val[dummy_id][fld] = 0.0

            ret_val[dummy_id][total_amount_fld] = subscription_date_amt_sum

            if deal_subscription_date_prd > rev_period and 'won_amount' in ret_val[dummy_id]:
                for prefix in ['', 'existing_pipe_', 'new_pipe_']:
                    if prefix + 'won_amount' in ret_val[dummy_id]:
                        ret_val[dummy_id][prefix + 'active_amount'] = dict(ret_val[dummy_id][prefix + 'won_amount'])
                        for node in ret_val[dummy_id][prefix + 'won_amount']:
                            ret_val[dummy_id][prefix + 'won_amount'][node] = 0.0
                            if prefix == '':
                                ret_val[dummy_id]['forecast'][node] = 0.0
                                ret_val[dummy_id]['eACV'][node] = 0.0
            ret_val[dummy_id]['__dummy_deal_rec'] = True

            if ret_val[dummy_id].get(renewal_amount_fld, 0) != 0:
                ret_val[dummy_id][global_amount_fld] = ret_val[dummy_id][renewal_amount_fld]
            elif ret_val[dummy_id].get(upsell_amount_fld, 0) != 0:
                ret_val[dummy_id][global_amount_fld] = ret_val[dummy_id][upsell_amount_fld]

    return ret_val

def generate_revenue_single_rec(rev_period, drilldown, close_date_fld, opp_id, res):
    ret_val = {}
    current_period_mne = current_period().mnemonic
    deal_rev_sched_amount = res.get('as_of_raw_rev_schedule_amounts', {})
    deal_rev_sched_date = res.get('as_of_raw_rev_schedule_dates', {})
    deal_close_date = res.get(close_date_fld, 'N/A')
    if deal_close_date =="N/A":
        ret_val[opp_id] = dict(res)
        return ret_val
    deal_close_date_prd = current_period(a_datetime=xl2datetime(deal_close_date)).mnemonic
    if rev_period < current_period_mne:
        revenue_segs = [seg for seg in res['__segs'] if
                        seg.startswith(drilldown + '#')]
        if deal_close_date_prd == rev_period:
            ret_val[opp_id] = dict(res)
            ret_val[opp_id]['rev_schedule_amount'] = deal_rev_sched_amount.get(rev_period, 0.0)
            if 'won_amount' in ret_val[opp_id]:
                for seg in revenue_segs:
                    ret_val[opp_id]['won_amount'][seg] = ret_val[opp_id]['rev_schedule_amount']
        elif rev_period in deal_rev_sched_amount:
            dummy_id = opp_id + '_' + rev_period
            ret_val[dummy_id] = dict(res)
            ret_val[dummy_id]['rev_schedule_amount'] = deal_rev_sched_amount.get(rev_period, 0.0)
            ret_val[dummy_id][close_date_fld] = deal_rev_sched_date.get(rev_period, 'N/A')
            ret_val[dummy_id]['__segs'] = [seg for seg in ret_val[dummy_id]['__segs'] if
                                           seg.startswith(drilldown + '#')]
            ret_val[dummy_id]['won_amount'] = {seg: ret_val[dummy_id]['rev_schedule_amount'] for seg in revenue_segs}
        else:
            ret_val = {}
    else:
        ret_val[opp_id] = dict(res)
        # for original deal record, revenue schedule amount should be taken from deal close date period
        ret_val[opp_id]['rev_schedule_amount'] = deal_rev_sched_amount.pop(deal_close_date_prd, 0.0)
        revenue_segs = [seg for seg in ret_val[opp_id]['__segs'] if
                        seg.startswith(drilldown + '#')]
        if 'won_amount' in ret_val[opp_id]: # For revenue segments won_amount will be revenue scheduled amount and not the actual amount
            for seg in revenue_segs:
                ret_val[opp_id]['won_amount'][seg] = ret_val[opp_id]['rev_schedule_amount']
        if deal_close_date_prd < current_period_mne : # changing won_amount to 0 for past quarter deals
            if 'won_amount' in ret_val[opp_id] and type(ret_val[opp_id]['won_amount'])==dict:
                for seg in ret_val[opp_id]['__segs']:
                    ret_val[opp_id]['won_amount'][seg] = 0
            else:
                ret_val[opp_id]['won_amount'] = 0


        # create record for every remaining revenue period
        for mne, amt in deal_rev_sched_amount.items():
            if mne >= current_period_mne and deal_close_date_prd != current_period_mne: # we don't need to create dummy record if deal_close_date_prd = current_period_mne as revenue scheduling is handled above for that case
                dummy_id = opp_id + '_' + mne
                ret_val[dummy_id] = dict(res)
                ret_val[dummy_id]['rev_schedule_amount'] = amt
                ret_val[dummy_id][close_date_fld] = deal_rev_sched_date.get(mne, 'N/A')
                ret_val[dummy_id]['__segs'] = [seg for seg in ret_val[dummy_id]['__segs'] if
                                               seg.startswith(drilldown + '#')]
                if mne > current_period_mne:        # making won_amount as 0 for future quarter dummy deals
                    ret_val[dummy_id]['won_amount'] ={seg:0 for seg in revenue_segs}
                else:
                    ret_val[dummy_id]['won_amount'] ={seg:ret_val[dummy_id]['rev_schedule_amount'] for seg in revenue_segs}
    return  ret_val

def generate_revenue_recs(rev_period, drilldown, close_date_fld, results):
    ret_val = {}
    current_period_mne = current_period().mnemonic
    for opp_id, res in results.items():
        deal_rev_sched_amount = res.get('as_of_raw_rev_schedule_amounts', {})
        deal_rev_sched_date = res.get('as_of_raw_rev_schedule_dates', {})
        deal_close_date = res.get(close_date_fld, 'N/A')
        # if close date is "N/A" then just return the records
        if deal_close_date == 'N/A':
            ret_val[opp_id] = dict(res)
            continue
        deal_close_date_prd = current_period(a_datetime=xl2datetime(deal_close_date)).mnemonic

        # when rev_period is in past
        if rev_period < current_period_mne:
            if deal_close_date_prd == rev_period:
                ret_val[opp_id] = dict(res)
                ret_val[opp_id]['rev_schedule_amount'] = deal_rev_sched_amount.get(rev_period, 0.0)
            elif rev_period in deal_rev_sched_amount:
                dummy_id = opp_id+'_'+rev_period
                ret_val[dummy_id] = dict(res)
                ret_val[dummy_id]['rev_schedule_amount'] = deal_rev_sched_amount.get(rev_period, 0.0)
                ret_val[dummy_id][close_date_fld] = deal_rev_sched_date.get(rev_period, 'N/A')
                ret_val[dummy_id]['__segs'] = [seg for seg in ret_val[dummy_id]['__segs'] if seg.startswith(drilldown+'#')]
            else:
                # do not create any record
                pass

        # when rev_period is current period
        else:
            # the deals with close date period in past would be ignored in caches run on forecast app end
            ret_val[opp_id] = dict(res)
            # for original deal record, revenue schedule amount should be taken from deal close date period
            ret_val[opp_id]['rev_schedule_amount'] = deal_rev_sched_amount.pop(deal_close_date_prd, 0.0)

            # create record for every remaining revenue period
            for mne, amt in deal_rev_sched_amount.items():
                if mne >= current_period_mne:
                    dummy_id = opp_id+'_'+mne
                    ret_val[dummy_id] = dict(res)
                    ret_val[dummy_id]['rev_schedule_amount'] = amt
                    ret_val[dummy_id][close_date_fld] = deal_rev_sched_date.get(mne, 'N/A')
                    ret_val[dummy_id]['__segs'] = [seg for seg in ret_val[dummy_id]['__segs'] if seg.startswith(drilldown+'#')]

    return ret_val


def generate_subscription_recs(rev_period, drilldown, close_date_fld, results):
    '''
    Subscription records are generated when the customer has subscription starting date and its amount associated with a deal.
    Example tenant - appannie.com
    '''
    ret_val = {}
    current_period_mne = current_period().mnemonic
    for opp_id, res in results.items():
        deal_rev_sched_amount = res.get('as_of_raw_rev_schedule_amounts', {})
        deal_rev_sched_date = res.get('as_of_raw_rev_schedule_dates', {})
        deal_close_date = res.get(close_date_fld, 'N/A')

        # if close date is "N/A" then just return the records
        if deal_close_date == 'N/A':
            ret_val[opp_id] = dict(res)
            ret_val[opp_id]['__segs'] =  [seg for seg in ret_val[opp_id]['__segs'] if not seg.startswith(drilldown+'#')]

            for mne, amt in deal_rev_sched_amount.items():
                dummy_id = opp_id+'_'+mne
                ret_val[dummy_id] = dict(res)
                ret_val[dummy_id]['rev_schedule_amount'] = amt
                ret_val[dummy_id][close_date_fld] = deal_rev_sched_date.get(mne, 'N/A')
                ret_val[dummy_id]['__segs'] = [seg for seg in ret_val[dummy_id]['__segs'] if seg.startswith(drilldown+'#')]

            continue

        deal_close_date_prd = current_period(a_datetime=xl2datetime(deal_close_date)).mnemonic
        # when rev_period is in past
        if rev_period < current_period_mne:
            ret_val[opp_id] = dict(res)
            ret_val[opp_id]['rev_schedule_amount'] = deal_rev_sched_amount.get(rev_period, 0.0)
            ret_val[opp_id]['__segs'] =  [seg for seg in ret_val[opp_id]['__segs'] if not seg.startswith(drilldown+'#')]

            if rev_period in deal_rev_sched_amount:
                dummy_id = opp_id+'_'+rev_period
                ret_val[dummy_id] = dict(res)
                ret_val[dummy_id]['rev_schedule_amount'] = deal_rev_sched_amount.get(rev_period, 0.0)
                ret_val[dummy_id][close_date_fld] = deal_rev_sched_date.get(rev_period, 'N/A')
                ret_val[dummy_id]['__segs'] = [seg for seg in ret_val[dummy_id]['__segs'] if seg.startswith(drilldown+'#')]

        # when rev_period is current period
        else:
            # the deals with close date period in past would be ignored in caches run on forecast app end
            ret_val[opp_id] = dict(res)
            # for original deal record, revenue schedule amount should be taken from deal close date period
            ret_val[opp_id]['rev_schedule_amount'] = deal_rev_sched_amount.get(deal_close_date_prd, 0.0)
            ret_val[opp_id]['__segs'] = [seg for seg in ret_val[opp_id]['__segs'] if not seg.startswith(drilldown+'#')]

            # create record for every remaining revenue period
            for mne, amt in deal_rev_sched_amount.items():
                if mne >= current_period_mne:
                    dummy_id = opp_id+'_'+mne
                    ret_val[dummy_id] = dict(res)
                    ret_val[dummy_id]['rev_schedule_amount'] = amt
                    ret_val[dummy_id][close_date_fld] = deal_rev_sched_date.get(mne, 'N/A')
                    ret_val[dummy_id]['__segs'] = [seg for seg in ret_val[dummy_id]['__segs'] if seg.startswith(drilldown+'#')]

    return ret_val



def add_additionl_revenue_flds(additional_rev_amount, close_date_fld, results):
    ret_val = dict(results)
    for opp_id, res in results.items():
        deal_close_date = res.get(close_date_fld, 'N/A')
        # if close date is "N/A" then bail
        if deal_close_date == 'N/A':
            continue
        deal_close_date_prd = current_period(a_datetime=xl2datetime(deal_close_date)).mnemonic
        for uip_fld, res_fld in additional_rev_amount.items():
            deal_raw_rev_amount = res.get('as_of_raw_'+uip_fld, {})
            ret_val[opp_id][res_fld] = deal_raw_rev_amount.get(deal_close_date_prd, 'N/A')

    return ret_val
def parse_mongo(crit):
    """
    Due to historical reasons, the UI passes a mongo query as a filter. This function
    attempts to take the mongo query and create a python function which can be used to
    evaluate a True/False result based on the equivalent in memory object.
    The mongo object the legacy code filters against is as follows:
    {
        "object": {
            "result": { <model result fields> },
            "uipfield": { <if a field name is missing standard time prefix the value is assumed to be as_of_>},
            "dimensions": { <if a field name is missing standard time prefix the value is assumed to be latest_> }
        }
    }
    The new in-memory object is of the form:
    {
        "res": { <model result fields> },
        "uip": { <all field names have standard time prefixes>}
    }
    """
    if not crit:
        return 'True'
    if isinstance(crit, basestring):
        return '=="%s"' % crit
    if not isinstance(crit, dict):
        return '==%s' % crit
    if len(crit) > 1:
        raise Exception('Invalid criterion: %s' % crit)
    key, vals = crit.items()[0]
    if key == '$and':
        guts = ' and '.join([parse_mongo(x) for x in vals])
    elif key == '$or':
        guts = ' or '.join([parse_mongo(x) for x in vals])
    elif key == '$in':
        return' in %s' % vals
    else:
        beg_str = "x"
        prefix = ""
        if key.startswith('object.'):
            key = key.replace('object.', '', 1)
        if key.startswith('results'):
            key = key.replace('results.', '', 1)
            beg_str = "x['res']"
        elif key.startswith('uipfield.'):
            key = key.replace('uipfield.', '', 1)
            beg_str = "x['uip']"
            prefix = "as_of_"
        elif key.startswith('dimensions.'):
            key = key.replace('dimensions.', '', 1)
            beg_str = "x['uip']"
            prefix = "latest_"
        guts = beg_str + ''.join(['.get("' + (add_prefix(x, prefix) if prefix else x) + '",{})'
                                  for x in key.split('.')]) + parse_mongo(vals)

    return '(%s)' % guts




def mongo_filter_to_python(crit):
    try:
        return eval('lambda x: ' + parse_mongo(crit))
    except Exception as e:
        logger.exception('ERROR: unable to parse result_filter: %s' % crit)
        return lambda x: False








def get_results_objects(
        ds_name,
        model_name,
        cache_key,
        stage_name=None,
        allowed_dimensions=None,  # TODO: make sure this is in the mongo query
        exec_time=None,
        drilldowns=None,
        get_results_from_as_of=None,
):
    ds_inst = Dataset.getByNameAndStage(ds_name, stage_name)
    if not ds_inst:
        raise GnanaError("dataset=%s not found!" % ds_name)
    if model_name not in ds_inst.models:
        raise GnanaError("dataset=%s stage=%s does not define model=%s" % (ds_name, stage_name, model_name))
    model_cls = ds_inst.get_model_class(model_name)
    res_cls = get_result_class(ds_inst,
                               model_cls,
                               cache_key=cache_key,
                               exec_time=exec_time,)

    run_config = res_cls.RunInfoClass.getByFieldValue('run_time_horizon', res_cls.run_time_horizon)
    if not run_config:
        raise GnanaError(
            "No run config found for ds_name=%s, model_name=%s, cache_key=%s, stage_name=%s" % (
                ds_name, model_name, cache_key, stage_name))
    time_horizon = get_time_horizon(as_of=run_config.as_of, begins=run_config.begins, horizon=run_config.horizon)
    model_inst = ds_inst.get_model_instance(model_name, time_horizon, drilldowns)
    criteria = {'object.run_time_horizon': res_cls.run_time_horizon}
    if get_results_from_as_of:
        criteria = {'$and': [criteria, {'last_modified_time': {'$gte': get_results_from_as_of}}]}
    logger.info("criteria %s", criteria )
    return res_cls, model_inst, ds_inst, criteria


COUNT_MISMATCH = "Result count in run_config(%s) is not matching record count(%s) in DB for %s."



def validate_results(res_cls, model_inst):
    run_time_horizon = res_cls.run_time_horizon
    encrypted_run_time_horizon = res_cls.run_time_horizon
    run_result = gnana_db.findDocuments(
         res_cls.RunInfoClass.getCollectionName(), {'object.run_time_horizon': encrypted_run_time_horizon})
    result_count = res_cls.find_count({'object.run_time_horizon': encrypted_run_time_horizon})

    try:
        for i in run_result:
            #             i = run_result.next()
            run_count = i['object'].get('run_count', None)
            if run_count is None:
                logger.warning("No result_count for cache_key %s. Returning invalidated results.", run_time_horizon)
            elif model_inst.produce_individual_result:
                if run_count != result_count:
                    if 'live' not in run_time_horizon:
                        logger.exception(COUNT_MISMATCH, run_count, result_count, run_time_horizon)
                    else:
                        logger.warning(COUNT_MISMATCH, run_count, result_count, run_time_horizon)
    except StopIteration:
        raise GnanaError("model run_config is not available.", 230)


def generate_subscription_single_rec(rev_period, drilldown, close_date_fld, opp_id, res, filt_config, viewgen_config, chs):
    ret_val = {}
    second_close_date_fld = filt_config.get('second_close_date_fld', 'as_of_DueDate')
    second_amt_fld = filt_config.get('second_amt_fld', 'as_of_RenewalBookings')
    amt_fld = filt_config.get('amt_fld', 'as_of_UpsellBookings')
    tot_amt_fld = filt_config.get('tot_amt_fld', '__tot_rev_amt')
    second_owner_id_fld = filt_config.get('second_owner_id_fld', None)
    filter_zero_amount = filt_config.get('filter_zero_amount', None)

    dict_flds = ['eACV', 'won_amount', 'lost_amount', 'active_amount', 'forecast']

    deal_close_date = res.get(close_date_fld, 'N/A')
    deal_second_close_date = res.get(second_close_date_fld, 'N/A')

    if second_owner_id_fld:
        dd_key = list(viewgen_config['drilldown_config'].keys())[0]
        owner_id_field = viewgen_config.get('hier_config', {}).get(dd_key, {}).get('leaf_field', 'N/A')

    try:
        deal_close_date_prd = current_period(a_datetime=xl2datetime(deal_close_date)).mnemonic
    except:
        deal_close_date_prd = 'N/A'
    try:
        deal_second_close_date_prd = current_period(a_datetime=xl2datetime(deal_second_close_date)).mnemonic
    except:
        deal_second_close_date_prd = 'N/A'

    differnt_segments = second_owner_id_fld and res[owner_id_field] != res[second_owner_id_fld]
    if differnt_segments:
        temp_res = dict(res)
        temp_res[owner_id_field] = temp_res[second_owner_id_fld]
        hier_segs = chs.get_segs(temp_res, None)
        node_segs = gimme_node_service(viewgen_config.get('node_config', {})).amend_nodes(temp_res, hier_segs, dd_key)
        second_segments = list(node_segs.get('dummy', []))
    elif second_owner_id_fld:
        second_segments = list(res['__segs'])
    if second_owner_id_fld:
        all_segments = list(set(second_segments+res['__segs']))

    if deal_close_date_prd != deal_second_close_date_prd:
        tot_amt = try_float(res[amt_fld]) + try_float(res[second_amt_fld])
        if deal_close_date_prd == 'N/A' or deal_close_date_prd >= rev_period:
            ret_val[opp_id] = dict(res)
            amt = try_float(ret_val[opp_id][amt_fld])
            ratio = amt / tot_amt if tot_amt else 0.0
            if second_owner_id_fld:
                ret_val[opp_id][amt_fld] = {}
                ret_val[opp_id][second_amt_fld] = {}
                ret_val[opp_id][tot_amt_fld] = {}
                for segment in res['__segs']:
                    ret_val[opp_id][amt_fld][segment] = amt
                    ret_val[opp_id][second_amt_fld][segment] = 0.0
                    ret_val[opp_id][tot_amt_fld][segment] = amt
                    for dict_fld in dict_flds:
                        if dict_fld in ret_val[opp_id]:
                            ret_val[opp_id][dict_fld][segment] *= ratio
            else:
                ret_val[opp_id][amt_fld] = amt
                ret_val[opp_id][second_amt_fld] = 0.0
                ret_val[opp_id][tot_amt_fld] = amt
            if filter_zero_amount and not amt:
                ret_val[opp_id]['__segs'] = []
                for dict_fld in dict_flds:
                    if dict_fld in ret_val[opp_id]:
                        ret_val[opp_id][dict_fld] = {}
        if deal_second_close_date_prd != 'N/A' and deal_second_close_date_prd >= rev_period:
            dummy_id = opp_id+'_'+deal_second_close_date_prd
            ret_val[dummy_id] = dict(res)
            ret_val[dummy_id][close_date_fld] = deal_second_close_date
            dummy_second_amt = try_float(ret_val[dummy_id][second_amt_fld])
            ratio = dummy_second_amt / tot_amt if tot_amt else 0.0
            ret_val[dummy_id]['__dummy_deal_rec'] = True
            if second_owner_id_fld:
                ret_val[dummy_id][amt_fld] = {}
                ret_val[dummy_id][second_amt_fld] = {}
                ret_val[dummy_id][tot_amt_fld] = {}
                for segment in second_segments:
                    ret_val[dummy_id][amt_fld][segment] = 0.0
                    ret_val[dummy_id][second_amt_fld][segment] = dummy_second_amt
                    ret_val[dummy_id][tot_amt_fld][segment] = dummy_second_amt
                    for dict_fld in dict_flds:
                        if dict_fld in ret_val[dummy_id]:
                            ret_val[dummy_id][dict_fld][segment] = res[dict_fld][res['__segs'][0]] * ratio
                ret_val[dummy_id]['__segs'] = list(second_segments)
            else:
                ret_val[dummy_id][amt_fld] = 0.0
                ret_val[dummy_id][second_amt_fld] = dummy_second_amt
                ret_val[dummy_id][tot_amt_fld] = dummy_second_amt
            if filter_zero_amount and not dummy_second_amt:
                ret_val[dummy_id]['__segs'] = []
    else:
        if deal_close_date_prd == 'N/A' or deal_close_date_prd >= rev_period:
            ret_val[opp_id] = dict(res)
            amt = try_float(ret_val[opp_id][amt_fld])
            second_amt = try_float(ret_val[opp_id][second_amt_fld])
            tot_amt = amt +second_amt
            ratio_1 = amt / tot_amt if tot_amt else 0.0
            ratio_2 = second_amt / tot_amt if tot_amt else 0.0
            if second_owner_id_fld:
                ret_val[opp_id][amt_fld] = {}
                ret_val[opp_id][second_amt_fld] = {}
                ret_val[opp_id][tot_amt_fld] = {}
                for segment in all_segments:
                    ret_val[opp_id][amt_fld][segment] = amt if segment in res['__segs'] else 0.0
                    ret_val[opp_id][second_amt_fld][segment] = second_amt if segment in second_segments else 0.0
                    ret_val[opp_id][tot_amt_fld][segment] = amt if segment in res['__segs'] else 0.0
                    for dict_fld in dict_flds:
                        if dict_fld in ret_val[opp_id]:
                            if segment not in second_segments:
                                ret_val[opp_id][dict_fld][segment] *= ratio_1
                    if segment in second_segments:
                        ret_val[opp_id][tot_amt_fld][segment] += second_amt
                        for dict_fld in dict_flds:
                            if dict_fld in ret_val[opp_id]:
                                if segment not in res['__segs']:
                                    ret_val[opp_id][dict_fld][segment] = res[dict_fld][res['__segs'][0]] * ratio_2
                ret_val[opp_id]['__segs'] = list(all_segments)
            else:
                ret_val[opp_id][amt_fld] = amt
                ret_val[opp_id][second_amt_fld] = second_amt
                ret_val[opp_id][tot_amt_fld] = amt + second_amt
            if filter_zero_amount:
                if not amt and not second_amt:
                    ret_val[opp_id]['__segs'] = []
                    for dict_fld in dict_flds:
                        if dict_fld in ret_val[opp_id]:
                            ret_val[opp_id][dict_fld] = {}
                elif not amt:
                    ret_val[opp_id]['__segs'] = list(second_segments)
                    for dict_fld in dict_flds:
                        if dict_fld in ret_val[opp_id]:
                            ret_val[opp_id][dict_fld] = {segment:val for segment,val in ret_val[opp_id][dict_fld].items() if segment in list(second_segments)}
                elif not second_amt:
                    ret_val[opp_id]['__segs'] = list(res['__segs'])
                    for dict_fld in dict_flds:
                        if dict_fld in ret_val[opp_id]:
                            ret_val[opp_id][dict_fld] = {segment:val for segment,val in ret_val[opp_id][dict_fld].items() if segment in list(res['__segs'])}

    return ret_val




def get_individual_results_generator(
        ds_name,
        model_name,
        cache_key,
        stage_name=None,
        allowed_dimensions=None,
        result_filter=None,
        result_fields=None,
        uip_fields=None,
        drilldowns=None,
        cached_records=None,
        prune_nulls=False,
        get_results_from_as_of=None,
        custom_fields = []
):
    """
    ds_name: name of dataset as string
    model_name: name of model as string
    cache_key: the cache key used to save the model (note: do not add custom_, it will be added internally)
    stage_name: name of stage for dataset as string
    allowed_dimensions: permission specification see "check_dimension_values()" for details.
    result_filter: None, means no filter, otherwise must be a dict which looks like a mongo query.
        Note that we will not actually use the mongo query but instead load all the results and then
        filter them in memory using a filtering function which will be created on the fly from the
        mongo query
    result_fields: list of fields to keep from the result part of the saved objects (default of None
        means keep everything)
    uip_fields: This is a list of fields to be returned with the result. Each element if the list can
        be one of the following:
        (i) a string: in which case it will be assumed to be the name to be returned and if missing
        an explicit time modifier will be assumed to be as_of_
        (ii) a tuple where the first element will be an arbitrary name to be returned and the second
        element MUST be explicitly marked with acceptable time modifier
        NOTE: Any field that is not found will cause a cache miss and can be expensive so be careful.
    drilldowns: Is polymorphic. Can be one of:
        (i) a list of drilldown definitions (tilde_separated field names). Empty list is acceptable input
            and should be used for calculating top-level summary results. Passing a list to the drilldown
            parameter will always cause "__segs" key to be on every view in the view lists returned.
        (ii) a string representing a perspective. Perspectives are useful for getting specific node
            results in our "node_id format": <tilde_separated_field_names>~::~<tilde_separated_field_values>
            Asking for perspective will cause the returned result to be just the view matching the
            perspective criteria (as opposed to a list of views).
        (iii) None will be defaulted to ".~::~summary" and return top level perspective
    cached_records: a list of things that look like db records which we got some other. If you pass these,
        we will skip the fetch-from-db step. Used in short_answers.py
    """
    res_cls, model_inst, ds_inst, criteria = get_results_objects(
        ds_name=ds_name,
        model_name=model_name,
        cache_key=cache_key,
        stage_name=stage_name,
        allowed_dimensions=allowed_dimensions,
        get_results_from_as_of=get_results_from_as_of,
    )
    time_horizon = model_inst.time_horizon
    validate_results(res_cls, model_inst)


    result_fields = None if result_fields is None else set(result_fields)
    if uip_fields is None:
        disp_uip_fld_map = {}
    else:
        uip_fields = [(x[0], x[1]) if isinstance(x, (tuple, list)) else (x, add_prefix(x, 'as_of_'))
                      for x in uip_fields]
        disp_uip_fld_map = dict(uip_fields)
        if len(disp_uip_fld_map) != len(uip_fields):
            logger.warning("WARNING: requested uip_fields were clobbered: %s",
                           set(uip_fields) - set(disp_uip_fld_map.items()))

    # for A mode reverse lookup
    rev_disp_map = {v: k for k, v in disp_uip_fld_map.items()}

    drilldown_config = ds_inst.params['general'].get('drilldown_config', {})
    # lets just do this up front since it now matters more
    hs_impl = drilldown_config.get('hs_impl', 'G')

    if hs_impl == 'A':
        if isinstance(drilldowns, basestring):
            perspective = drilldowns
        else:
            perspective = None
        # we don't actually care about the drilldowns in the new version,
        # or at least this flavor of it
        drilldowns = None
    else:
        if drilldowns is None or drilldowns == ".~::~summary":
            perspective = ".~::~summary"
            drilldowns = None
        elif isinstance(drilldowns, basestring):
            perspective = drilldowns
            drilldowns = [perspective.split('~::~', 1)[0]]
        else:  # it must be list of tilde-separated drilldown fields
            perspective = None

    drilldown_config['drilldowns'] = drilldowns

    full_uip_fld_map = disp_uip_fld_map.copy()
    if isinstance(drilldowns, list):
        for drilldown in drilldowns:
            if drilldown == ".":
                continue
            drilldown = drilldown.split('~')
            full_uip_fld_map.update({fld: add_prefix(fld)
                                     for fld in drilldown})

    # TODO: SPLITS, this need to work on 'standardized Ind Res format' - whatever that is :)
    filter_fn = mongo_filter_to_python(result_filter) if result_filter is not None else lambda x: True
    # TODO we actually overwrite this in viewgen anyway, prob get rid of?
    splitter_config = model_inst.config.get("splitter_config", None)


    # if we're in awesome mode, aka new microservice mode, do all the cool stuff
    if hs_impl == 'A':
        view_gen = CoolerViewGeneratorService(hier_asof=time_horizon.as_of, perspective=perspective)
    else:
        view_gen = ViewGeneratorService(perspective=perspective,
                                        drilldown_config=drilldown_config,
                                        filter_fn=filter_fn,
                                        allowed_dims=allowed_dimensions,
                                        uip_fld_map=full_uip_fld_map,
                                        splitter_config=splitter_config,
                                        base_ts=time_horizon.as_of)



    if cached_records is None:
        all_results = res_cls.findDocuments(criteria)
        print(f"Total records fetched from DB: {len(list(res_cls.findDocuments(criteria)))}")

    else:
        all_results = cached_records

    def flatten_view(view):
        # TODO: Its a pretty weird place to definie this, muy malo
        uips = view['uip']
        if result_fields is None:
            ret_view = view['res']
        else:
            ret_view = {k: v for k, v in view['res'].items()
                        if k in result_fields}
        ret_view.update({disp_fld: uips[std_fld]
                         for disp_fld, std_fld in disp_uip_fld_map.items()
                         if not (prune_nulls and uips[std_fld] == 'N/A')})
        if isinstance(drilldowns, list):
            ret_view['__segs'] = view['__segs']
        return ret_view

    first_record = True
    record_dict = {}
    ext_id_list = []
    for record in all_results:
        obj = record['object']
        extid = obj['extid']
        if first_record:
            # These steps are done to avoid calling add_prefix multiple times.
            # This assumes that all results for this cache key have the same fields cached.
            uip_flds_to_std_fld_map = {x: add_prefix(x, 'as_of_') for x in obj.get('uipfield', {})}
            dim_flds_to_std_fld_map = {x: add_prefix(x) for x in obj.get('dimensions', {})}
            cached_flds = set(uip_flds_to_std_fld_map.values()) | set(dim_flds_to_std_fld_map.values())
            missing_flds = set(full_uip_fld_map.values()) - cached_flds - view_gen.foreign_fields
            if custom_fields:
                missing_flds = set([fld for fld in list(missing_flds) if fld in custom_fields])
            if missing_flds:
                logger.warning(
                    "WARNING: Getting results for model %s with cache_key %s required going back to uip for %s",
                    model_name, cache_key, missing_flds)
            first_record = False
        if missing_flds:
            ext_id_list.append(record['object']['extid'])
        record_dict[extid] = {'uipfield' : obj['uipfield'],
                              'dimensions': obj['dimensions'],
                              'results': obj['results']}
    if ext_id_list:
        args = dict(dataset=ds_inst.name, extid_list=ext_id_list)
        record_iterator = sec_context.etl.uip('KnownUIPRecordIterator',**args)
    else:
        record_iterator = record_dict.keys()
    updated_ids = []

    segment_config = ds_inst.models['common'].config.get('segment_config', {})
    rev_schedule_config = ds_inst.models['common'].config.get('rev_schedule_config', {})
    rev_period = current_period(epoch(time_horizon.begins)).mnemonic
    if rev_schedule_config.get('prd_rev_schedule', False) and rev_schedule_config.get('prd_subs_schedule_single_rec', False):
        drilldown = rev_schedule_config.get('drilldown', 'Revenue')
        close_date_fld = rev_schedule_config.get('close_date_fld', 'CloseDate')
        filt_config = rev_schedule_config.get('filt_config', {})
        viewgen_config = ds_inst.models['common'].config.get('viewgen_config', {})
        second_owner_id_fld = filt_config.get('second_owner_id_fld', None)
        chs = None
        if second_owner_id_fld:
            chs = CoolerHierarchyService(viewgen_config.get('hier_config', {}), None)
    if rev_schedule_config.get('prd_rev_schedule', False) and rev_schedule_config.get('expiration_date_renewals_rec', False):
        renewal_drilldown = rev_schedule_config.get('renewal_drilldown', 'Renewal')
        close_date_fld = rev_schedule_config.get('close_date_fld', 'CloseDate')
        expiration_date_fld = rev_schedule_config.get('expiration_date_fld', 'ExpirationDate')
        type_fld = rev_schedule_config.get('type_fld', 'Type')
        renewal_vals = rev_schedule_config.get('renewal_vals', ['Renewal'])
        
        ##Druva Splits
        viewgen_config = ds_inst.models['common'].config.get('viewgen_config', {})
        splitted_fields = []
        if viewgen_config['split_config']:
            split_config = viewgen_config['split_config'][list(viewgen_config['split_config'])[0]]
            for _, dtls in split_config.items():
                if type(dtls) != list:
                    dtls = [dtls]
                for element in dtls:
                    splitted_fields += element['num_fields']
                    
    if rev_schedule_config.get('prd_rev_schedule', False) and rev_schedule_config.get('github_metered_billing', False):
        renewal_drilldown = rev_schedule_config.get('renewal_drilldown', 'Renewal')
        close_date_fld = rev_schedule_config.get('close_date_fld', 'CloseDate')
        expiration_date_fld = rev_schedule_config.get('expiration_date_fld', 'ExpirationDate')
        type_fld = rev_schedule_config.get('type_fld', 'Type')
        renewal_vals = rev_schedule_config.get('renewal_vals', ['Renewal'])
    if rev_schedule_config.get('prd_rev_schedule', False):
        drilldown = rev_schedule_config.get('drilldown', 'Revenue')
        close_date_fld = rev_schedule_config.get('close_date_fld', 'CloseDate')
        filt_config = rev_schedule_config.get('filt_config', {})

    for record in record_iterator:
        if missing_flds:
            obj = record_dict[record.ID]
            extid = record.ID
        else:
            obj = record_dict[record]
            extid = record
        if get_results_from_as_of:
            updated_ids.append(extid)
        std_uips = {v: obj['uipfield'].get(k, 'N/A') for k, v in uip_flds_to_std_fld_map.items()}
        std_uips.update({v: obj['dimensions'].get(k, 'N/A') for k, v in dim_flds_to_std_fld_map.items()})

        if missing_flds:
            std_uips.update({fname: record.getCacheValueF(fname, time_horizon)[1]
                             for fname in missing_flds})

        flat_rec = {'res': obj['results'], 'uip': std_uips}


        view_list = view_gen.get_views(flat_rec)
        if not view_list:
            continue

        # do some conditional formatting stuff, a bit janky
        # TODO: this should be made a part of the viewgen imo
        if hs_impl == 'A':
            output = view_gen.format_output(view_list, rev_disp_map)
        else:
            view_list = list(flatten_view(view) for view in view_list)
            output = view_list[0] if perspective else view_list

        # # Setting win prob as N/A
        # output['win_prob'] = output.get('win_prob', 'N/A')

        if rev_schedule_config.get('prd_rev_schedule', False) and rev_schedule_config.get('prd_subs_schedule_single_rec', False):
            output_dict = generate_subscription_single_rec(rev_period, drilldown, close_date_fld, extid, output, filt_config, viewgen_config, chs)
            for extid, output in output_dict.items():
                if custom_fields:
                    yield (extid, {key: output[key] for key in custom_fields if key in output})
                else:
                    yield (extid, output)
        elif rev_schedule_config.get('prd_rev_schedule', False) and rev_schedule_config.get('expiration_date_renewals_rec', False):
            output_dict = generate_expiration_date_renewal_rec(rev_period, 
                                                               renewal_drilldown, 
                                                               close_date_fld, 
                                                               expiration_date_fld, 
                                                               type_fld, 
                                                               renewal_vals, 
                                                               extid, output, 
                                                               splitted_fields)
            for extid, output in output_dict.items():
                if custom_fields:
                    yield (extid, {key: output[key] for key in custom_fields if key in output})
                else:
                    yield (extid, output)
        elif rev_schedule_config.get('prd_rev_schedule', False) and rev_schedule_config.get('github_metered_billing', False):
            output_dict = generate_expiration_date_renewal_rec_github(rev_period, renewal_drilldown, close_date_fld,
                                                                      expiration_date_fld, type_fld, renewal_vals, segment_config,
                                                                      rev_schedule_config,
                                                                      extid, output)
            for extid, output in output_dict.items():
                if custom_fields:
                    yield (extid, {key: output[key] for key in custom_fields if key in output})
                else:
                    yield (extid, output)
        elif rev_schedule_config.get('prd_rev_schedule', False) and rev_schedule_config.get('appannie_prd_rev_schedule', False):
            output_dict = generate_appannie_dummy_recs(rev_period, rev_schedule_config, extid, output)
            for extid, output in output_dict.items():
                if custom_fields:
                    yield (extid, {key: output[key] for key in custom_fields if key in output})
                else:
                    yield (extid, output)
        elif rev_schedule_config.get('prd_rev_schedule', False) and rev_schedule_config.get('prd_subs_schedule_same_dd', False):
            period = current_period(epoch(time_horizon.begins)).mnemonic
            qperiod, mperiod, is_curr_q = janky_period_parser(period)
            rev_period = mperiod if mperiod else qperiod
            if not isinstance(drilldown, list):
                drilldown = [drilldown]
            output_dict = generate_subscription_same_dd_single_rec(rev_period, drilldown, close_date_fld, filt_config, extid, output)
            for extid, output in output_dict.items():
                if custom_fields:
                    yield (extid, {key: output[key] for key in custom_fields if key in output})
                else:
                    yield (extid, output)
        elif rev_schedule_config.get('prd_rev_schedule',False) and rev_schedule_config.get('nuance_prd_rev_schedule',False):
            output_dict = generate_revenue_single_rec(rev_period, drilldown, close_date_fld, extid, output)
            for extid, output in output_dict.items():
                if custom_fields:
                    yield (extid, {key: output[key] for key in custom_fields if key in output})
                else:
                    yield (extid, output)
        else:
            if custom_fields:
                yield (extid, {key: output[key] for key in custom_fields if key in output})
            else:
                yield (extid, output)

    if get_results_from_as_of:
        #to add the deleted ids as well in the response
        dd_criteria = {'$and': [{'object.run_time_horizon': 'ZZZ_DELETED'},
                                {'last_modified_time': {'$gte': get_results_from_as_of}}]}
        dd_results = res_cls.findDocuments(dd_criteria)
        dd_extids= set([rec['object']['extid'] for rec in dd_results])
        logger.info('Updated ids in this run are : %s' % updated_ids)
        logger.info('Deleted ids in this run are : %s' % dd_extids)
        for extid in dd_extids:
            if extid in updated_ids:
                continue
            yield (extid, {'is_deleted':True})


def add_prefix(aFeature, default_prefix='latest_'):
    # Eswar claims this optimization gives a 5% performance improvement in model runtime.
    # Be careful to choose prefixes that don't invalidate this optimization.
    if aFeature[0] == 'a':
        if(aFeature[:9] == 'as_of_fv_' or
                aFeature[:11] == 'atbegin_fv_' or
                aFeature[:13] == 'athorizon_fv_' or
                aFeature[:6] == 'as_of_' or
                aFeature[:8] == 'atbegin_' or
                aFeature[:10] == 'athorizon_'):
            return aFeature
    else:
        if aFeature[:7] in ['latest_', 'frozen_']:
            return aFeature
    return default_prefix + aFeature



def janky_period_parser(period):
    '''
    Our results are horribly formatted, so we have to parse out extra month index
    junk if we want to get results for monthly periods.
    '''
    curr_q = current_period().mnemonic
    if 'Q' in period:
        return period, None, period==curr_q
#originol code
#       return period, None, period == curr_q

    return current_period(epoch(int(period[:4]), int(period[4:6]), 1)).mnemonic, period, False

def get_latest_chipotle():
    #gets the latest set of chipotle results, should always be curr q
    raw_chip_runs = get_individual_result_runs({}, model='bookings_rtfm', dataset='OppDS')
    # we don't care about live_monthly
    try:
        chip_run = next(run for run in raw_chip_runs if run['name'] == 'live')
    except StopIteration:
        logger.warning('No chipotle found')
        return None, None
    mnem, ck = chip_run['as_of_mnemonic'], chip_run['name']
    exec_time = sec_context.details.get_flag('molecule_status', 'rtfm', {}).get('last_execution_time', None)
    logger.info("Live execution time is {}".format(exec_time))
    return (exec_time, ck) if exec_time else (None, None)



def get_individual_result_runs(inputs, **kwargs):
    if not inputs:
        inputs = QueryDict('')
    stage = inputs.get('stage')
    # can be sandbox also
    stage = inputs.get('sandbox')
    runsDateInfo = inputs.get('runsDateInfo', False)

    if isinstance(inputs, QueryDict):
        make_int = lambda a: [int(i) for i in a]
        mnemonic = inputs.getlist('mnemonic')
        begin_month_mnemonic = make_int(inputs.getlist('begin_month_mnemonic', []))
        as_of_month_mnemonic = make_int(inputs.getlist('as_of_month_mnemonic', []))
        horizon_month_mnemonic = make_int(inputs.getlist('horizon_month_mnemonic', []))

    else:
        mnemonic = inputs.get('mnemonic', [])
        begin_month_mnemonic = inputs.get('begin_month_mnemonic', [])
        as_of_month_mnemonic = inputs.get('as_of_month_mnemonic', [])
        horizon_month_mnemonic = inputs.get('horizon_month_mnemonic', [])

    ds = Dataset.getByNameAndStage(kwargs.get('dataset'),
                                   stage)
    if not ds:
        logger.exception("dataset %s stage %s is not there" % (kwargs.get('dataset'), stage))
        return HttpResponseNotFound()

    model_name = kwargs.get("model")
    if model_name not in ds.models:
        logger.exception("no such model found %s" % model_name)
        return HttpResponseNotFound("No such model found")

    model_cls = ds.get_model_class(model_name)

    if not model_cls:
        logger.exception("model=%s not found" % model_name)
        return HttpResponseNotFound("model=%s not found" % model_name)
    criteria = []

    for mnem, fld_name in [(mnemonic, 'as_of_mnemonic'),
                           (begin_month_mnemonic, 'begin_month_mnemonic'),
                           (as_of_month_mnemonic, 'as_of_month_mnemonic'),
                           (horizon_month_mnemonic, 'horizon_month_mnemonic')]:
        if mnem:
            if len(mnem) > 1:
                criteria.append({'$or': [{'object.%s' % fld_name: x} for x in mnem]})
            else:
                criteria.append({"object.%s" % fld_name: mnem[0]})
    res_cls = get_result_class(ds, model_cls)
    criteria = {"$and": criteria} if criteria else {}
    runs = gnana_db.findDocuments(res_cls.RunInfoClass.getCollectionName(), criteria)
    result = []
    for r in runs:
        run_time_horizon = r['object']['run_time_horizon']

        if run_time_horizon.startswith('std_'):
            logger.warning('Please manually remove your old std run. rth=%s', run_time_horizon)
        elif run_time_horizon.startswith('custom_'):
            runInfo = {'type': 'custom',
                       'name': run_time_horizon[7:],
                       'as_of_mnemonic': r['object'].get('as_of_mnemonic', None),
                       'as_of_month_mnemonic': r['object'].get('as_of_month_mnemonic', None),
                       'begin_month_mnemonic': r['object'].get('begin_month_mnemonic', None),
                       'horizon_month_mnemonic': r['object'].get('horizon_month_mnemonic', None)}

            if runsDateInfo:
                runInfo['beginsDate'] = datetime2epoch(
                    dateUtils.from_db(r['object'].get('begins', None)))
                runInfo['asOfDate'] = datetime2epoch(
                    dateUtils.from_db(r['object'].get('as_of', None)))
                runInfo['horizonDate'] = datetime2epoch(
                    dateUtils.from_db(r['object'].get('horizon', None)))
                runInfo['beginsDateStr'] = epoch2datetime(
                    datetime2epoch(dateUtils.from_db(
                        r['object'].get('begins', None)))).isoformat()
                runInfo['asOfDateStr'] = epoch2datetime(
                    datetime2epoch(dateUtils.from_db(
                        r['object'].get('as_of', None)))).isoformat()
                runInfo['horizonDateStr'] = epoch2datetime(
                    datetime2epoch(dateUtils.from_db(
                        r['object'].get('horizon', None)))).isoformat()
            result.append(runInfo)
        else:
            raise GnanaError('Unexpected cache key {}'.format(run_time_horizon))

    return result




def individual_results_parser(qperiod, mperiod=None):
    '''
    Our IMR uses only quarter mnemonic then uses "month indexes" within the results to denote
    if it's a monthly run. This helps parse that and returns the runs we care about.
    TODO: This format is dumb. Revamp this if/when we rework the IMR format.
    '''
    raw_runs = get_individual_result_runs({'mnemonic': [qperiod]}, model='existingpipeforecast', dataset='OppDS')
    runs = [run for run in raw_runs if run['name'].startswith('Reporting') and run['name'].endswith('235959')]
    output = []
    for run in runs:
        ck = run['name']
        # we're just gonna do janky string parsing
        _, begstr, asofstr, horstr = ck.split('_')
        asof = epoch(asofstr).as_epoch()
        monthly = begstr[:6] == horstr[:6]
        # if you're not looking for monthly, and the run isn't monthly
        # or you are monthly, the run is monthly, and your indices match
        if (mperiod is None and not monthly) or (mperiod is not None and monthly and begstr[:6] == mperiod):
            output.append({'ck': ck, 'asof': asof})
    return sorted(output, key=lambda x: x['asof'])



def get_latest_daily(qperiod, mperiod, timestamp=None):
    '''
    Given args, determine the latest run that fits the criteria

    Arguments:
        qperiod {str} - The quarterly mnemonic provided
        mperiod {str} - The monthly mnemonic provided (None if period is quarterly)
        timestamp {int} - A cutoff for the latest value
    '''
    runs = individual_results_parser(qperiod, mperiod)
    if not runs:
        logger.warning('No runs found')
        return None, None
    if timestamp is None:
        return runs[-1]['asof'], runs[-1]['ck']
    # period mode, just grab last
    # if no cks pass criteria
    filt_runs = [run for run in runs if run['asof'] <= timestamp]
    try:
        return filt_runs[-1]['asof'], filt_runs[-1]['ck']
    except:
        logger.warning('No runs pass timestamp criteria. ts: {}, runs: {}'.format(timestamp, runs))
        return None, None

def deals_results_by_period(periods, include_uip=True, node=None, get_results_from_as_of=0, fields=[], opp_ids=[],
                            return_files_list=False, return_chipotle_files_list=False):


    '''
    Takes a period list and for each period looks up the latest run. If the period is for the
    current quarter, it checks whether or not the latest run is a chipotle and will use that instead.

    Input:
        periods {list of str} - A list of periods to get the latest results for
        return_chipotle_files_list {boolean} - If set, the result will contain S3 files names instead of deal result data.[Reserved for chipotle task only]
    Output:
        A dictionary of each period and the corresponding latest results, plus metadata
        {mnem:
            {
                'results': latest model results,
                'ck': cache key,
                'timestamp': the asof of the results returned
                'is_curr_q': if the results are for the current quarter,
            },
        }
    '''
    results = {}
    new_file_name_results = {}
    buffer_time = 3 * 60 * 60 * 1000
    for period in periods:
        results[period] = {}
        qperiod, mperiod, is_curr_q = janky_period_parser(period)
        asof, ck = get_latest_daily(qperiod, mperiod)
        model = 'existingpipeforecast'
        year, month, day = datetime.datetime.fromtimestamp(get_results_from_as_of / 1000,
                                                           tz=datetime.timezone.utc).strftime("%Y,%m,%d").split(",")
        if return_files_list:
            file_name = '/'.join([sec_context.name, CNAME, ck])

            counter = 0
            if gnana_storage.if_exists(file_name + '_' + str(counter) + '.csv', 'daily-results'):
                results[period] = [file_name + '_' + str(counter) + '.csv']
                counter += 1
                while gnana_storage.if_exists(file_name + '_' + str(counter) + '.csv', 'daily-results'):
                    results[period].append(file_name + '_' + str(counter) + '.csv')
                    counter += 1
            else:
                if gnana_storage.if_exists(file_name + '.csv', 'daily-results'):
                    results[period] = [file_name + '.csv']
                else:
                    results[period] = []
            return results

        if is_curr_q:
            chip_asof, chip_ck = get_latest_chipotle()
            if chip_asof and ((chip_asof + buffer_time) > asof or get_results_from_as_of):
                asof, ck, model = chip_asof, chip_ck, 'bookings_rtfm'
        if ck is None:
            logger.warning('No runs found for period: {}'.format(period))
            continue
        if return_chipotle_files_list:

            file_name = "/".join(
                ["chipotle-results", sec_context.name, year, month, day, f'chipotle_{get_results_from_as_of}'])

            counter = 0
            if gnana_storage.if_exists(file_name + '_' + str(counter) + '.json', 'daily-results'):
                results[period] = [file_name + '_' + str(counter) + '.json']
                counter += 1
                while gnana_storage.if_exists(file_name + '_' + str(counter) + '.json', 'daily-results'):
                    results[period].append(file_name + '_' + str(counter) + '.json')
                    counter += 1
            else:
                if gnana_storage.if_exists(file_name + '.json', 'daily-results'):
                    results[period] = [file_name + '.json']
                    return results

        if return_chipotle_files_list:
            file_name_new = "/".join(
                ["chipotle-results", sec_context.name, year, month, day, f'chipotle_{get_results_from_as_of}']) + '.json'
        else:
            file_name_new = '/'.join([sec_context.name, CNAME, ck]) + '.csv'
        if_exists = gnana_storage.if_exists(file_name_new, 'daily-results')
        fields_to_fetch = None

        if ck.startswith('live') or not if_exists:
            oppds = Dataset.getByNameAndStage(name='OppDS')
            win_score_config = oppds.models['common'].config.get('win_score_config', {})
            rev_schedule_config = oppds.models['common'].config.get('rev_schedule_config', {})
            drilldown = rev_schedule_config.get('drilldown', 'Revenue')
            close_date_fld = rev_schedule_config.get('close_date_fld', 'CloseDate')
            filt_config = rev_schedule_config.get('filt_config', {})

            fields_to_fetch = fields if ck.startswith('live') else None

            if fields_to_fetch and rev_schedule_config.get('prd_rev_schedule', False):
                if rev_schedule_config.get('prd_subs_schedule_same_dd', False):
                    if filt_config.get('terminal_fate_fld', 'as_of_terminal_fate') not in fields_to_fetch:
                        fields_to_fetch.append(filt_config.get('terminal_fate_fld', 'as_of_terminal_fate'))

            if fields_to_fetch:
                for field in win_score_config.get('required_fields', []):
                    fields.append(field)

            # TODO: make this return a dict, instead of list of dicts, splits will be fixed later
            results[period]['results'] = retrieve_deals(model, ck, include_uip, node,
                                                        get_results_from_as_of=get_results_from_as_of,
                                                        fields=fields_to_fetch)

            # if the tenant has revenue schedule
            if rev_schedule_config.get('prd_rev_schedule', False):
                rev_period = mperiod if mperiod else qperiod
                if rev_schedule_config.get('prd_subs_schedule_same_dd', False):
                    # condition is handled in the individual results generator
                    pass
                elif rev_schedule_config.get('appannie_prd_rev_schedule', False):
                    pass
                elif rev_schedule_config.get('prd_subs_schedule', False):
                    results[period]['results'] = generate_subscription_recs(rev_period, drilldown, close_date_fld, results[period]['results'])
                elif rev_schedule_config.get('prd_subs_schedule_single_rec', False):
                    pass
                elif rev_schedule_config.get('expiration_date_renewals_rec', False):
                    pass
                elif rev_schedule_config.get('github_metered_billing', False):
                    pass
                elif rev_schedule_config.get('nuance_prd_rev_schedule', False):
                    pass
                else:
                    results[period]['results'] = generate_revenue_recs(rev_period, drilldown, close_date_fld, results[period]['results'])

            # create additional revenue amount fields
            additional_rev_amount = rev_schedule_config.get('additional_rev_amount', {})
            if additional_rev_amount:
                results[period]['results'] = add_additionl_revenue_flds(additional_rev_amount, close_date_fld, results[period]['results'])

            results[period]['is_curr_q'] = is_curr_q
            results[period]['timestamp'] = asof
            results[period]['ck'] = ck
            results[period]['results'] = transform_score(results[period]['results'], win_score_config,
                                                         asof)  # to transform win scores
            for deal, dtls in list(results[period]['results'].items()):
                dtls['win_prob'] = dtls.get('win_prob', 'N/A')
        if not if_exists and return_chipotle_files_list:
            logger.info("Saving {} to daily-results bucket in s3".format(file_name_new))
            gnana_storage.save_daily_results_to_s3(file_name_new, results[period])
        if gnana_storage.if_exists(file_name_new, 'daily-results') and return_chipotle_files_list:
            results[period] = [file_name_new]

        if not ck.startswith('live'):
            if not if_exists:
                logger.info("Saving {} to daily-results bucket in s3".format(file_name_new))
                print("Saving {} to daily-results bucket in s3".format(file_name_new), results[period])
                #gnana_storage.save_daily_results_to_s3(file_name_new, results[period])
            if if_exists:
                logger.info("Reading {} from daily-results bucket in s3".format(file_name_new))
                results[period] = gnana_storage.read_daily_results_from_s3(file_name_new)
            if fields:
                new_results = {}
                for deal in results[period]['results']:
                    new_results[deal] = {}
                    for fld in fields:
                        if fld in results[period]['results'][deal]:
                            new_results[deal][fld] = results[period]['results'][deal][fld]
                results[period]['results'] = new_results

    return results




def retrieve_deals(model, cache_key, include_uip=True, node=None, get_results_from_as_of=0, fields = []):
    #with a given model and cache key, grab the results via the results generator
    tenant = sec_context.details
    tc = tenant.get_config(category='forecast', config_name='tenant')
    ds = Dataset.getByNameAndStage(name='OppDS')

    uips = ds.params['general']['uipfield']
    if 'req_uipfield' in ds.params['general'].keys():
        uips = ds.params['general']['req_uipfield']

    dimensions = ds.params['general']['dimensions']
    if include_uip:
        uip_fields = list(uips) + [(x, add_prefix(x)) for x in dimensions]
    else:
        uip_fields = None


    result_fields = ['win_prob', 'eACV', 'won_amount', 'lost_amount',
                     'scaled_score', 'active_amount', 'forecast',
                     'existing_pipe', 'projected_win_prob', 'group_results', 'model_features', 'cd_results',
                     'win_prob_threshold', 'winscoreprojections'
                     ]



    allowed_dimensions = '*'
    #drilldowns = ds.params['general']['weekly_report_dimensions']
    prune_nulls = tc.get('prune_nulls', False)
    dd_arg = node if node is not None else []

    try:
        ret_val = dict(get_individual_results_generator(
            ds_name='OppDS',
            model_name=model,
            cache_key=cache_key,
            uip_fields=uip_fields,
            result_fields=result_fields,
            allowed_dimensions=allowed_dimensions,
            drilldowns=dd_arg,
            prune_nulls=prune_nulls,
            get_results_from_as_of=get_results_from_as_of,
            custom_fields = fields
        ))

    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        err_msg = traceback.format_exception(exc_type, exc_value, exc_traceback)
        raise Exception("ERROR: Could not retrieve results for %s, %s\n%s\n%s",
                        model, cache_key, e, err_msg)

    # Need to log error only if error is present in ret_val
    if len(ret_val.keys())>0 and '_error' in list(ret_val.keys())[0]:
        logger.error('ERROR: No deals available for model {}, ck {}'.format(model, cache_key))
        raise Exception('ERROR: No deals available for model {}, ck {}'.format(model, cache_key))
        # Need to log warning if ret_val is empty
    if not ret_val:
        # Log warning if get_results_from_as_of is not passed in the API call
        if get_results_from_as_of==0:
            logger.warning('No deals available for model {}, ck {}'.format(model, cache_key))
        # Log warning if get_results_from_as_of date is more than 14 days before today's date
        elif get_results_from_as_of!=0 and ( epoch() - epoch(get_results_from_as_of) ).days>14:
            logger.warning('No deals available for model {}, ck {}'.format(model, cache_key))
        # No warning needed if above conditions are not met

    return ret_val


#deals_results_by_timestamp
def deals_results_by_timestamp(period, timestamps, include_uip=True, node=None, get_results_from_as_of=0, fields=[],
                               opp_ids=[], changed_deals=[], force_uip_and_hierarchy=False, allow_live=True,
                               return_files_list=False):
    '''
    Takes a period and a list of timestamps. The individual results are filtered to that period,
    then the latest result without going over that timestamp is returned. Will consider chipotle timestamp
    if it's the current quarterly period.

    get_results_from_as_of is a peculiar case of deal_results where we need the results related to changed deals only.

    Arguments:
        period {str} - A string representation of the period to limit the results to
        timestamps {list of int} - A list of timestamps to return results for
    Output:
        A dictionary of each period and the corresponding latest results, plus metadata
        {timestamp:
            {
                'results': latest model results,
                'ck': cache key,
                'timestamp': the asof of the results returned
                'is_curr_q': if the results are for the current quarter,
            },
        }
    '''
    results = {}
    buffer_time = 3 * 60 * 60 * 1000
    qperiod, mperiod, is_curr_q = janky_period_parser(period)

    if get_results_from_as_of and not changed_deals:
        asof, ck = get_latest_daily(qperiod, mperiod)
        model = 'existingpipeforecast'
        if is_curr_q:
            chip_asof, chip_ck = get_latest_chipotle()
            if chip_asof and ((chip_asof + buffer_time) > asof or get_results_from_as_of):
                asof, ck, model = chip_asof, chip_ck, 'bookings_rtfm'
                changed_deals.extend(list(retrieve_deals(model, ck, include_uip, node, get_results_from_as_of=get_results_from_as_of, fields=fields).keys()))


    for timestamp in timestamps:
        results[timestamp] = {}
        asof, ck = get_latest_daily(qperiod, mperiod, timestamp)
        model = 'existingpipeforecast'
        if return_files_list:
            file_name = '/'.join([sec_context.name, CNAME, ck])
            counter = 0
            if gnana_storage.if_exists(file_name + '_' + str(counter) + '.csv', 'daily-results'):
                results[timestamp] = [file_name + '_' + str(counter) + '.csv']
                counter += 1
                while gnana_storage.if_exists(file_name + '_' + str(counter) + '.csv', 'daily-results'):
                    results[timestamp].append(file_name + '_' + str(counter) + '.csv')
                    counter += 1
            else:
                if gnana_storage.if_exists(file_name + '.csv', 'daily-results'):
                    results[timestamp] = [file_name + '.csv']
                else:
                    results[timestamp] = []
            return results
        # If there are no dailies that pass the timestamp criteria, chipotle can't magically have worked
        # this may be buggy around BOQ?
        if ck is None:
            logger.warning('No runs found for period: {}'.format(timestamp))
            continue
        if is_curr_q and allow_live:
            chip_asof, chip_ck = get_latest_chipotle()
            if chip_asof and ((chip_asof + buffer_time) > asof) and (chip_asof < timestamp):
                asof, ck, model = chip_asof, chip_ck, 'bookings_rtfm'

        file_name = '/'.join([sec_context.name, CNAME, ck]) + '.csv'
        if_exists = gnana_storage.if_exists(file_name, 'daily-results')
        fields_to_fetch = None

        if force_uip_and_hierarchy and if_exists:
            logger.info("Deleting {} from daily-results bucket in s3".format(file_name))
            gnana_storage.delete_daily_results_from_s3(file_name)
            if_exists = False

        if ck.startswith('live') or not if_exists:
            oppds = Dataset.getByNameAndStage(name='OppDS')
            win_score_config = oppds.models['common'].config.get('win_score_config', {})

            fields_to_fetch = fields if ck.startswith('live') else None
            if fields_to_fetch:
                for field in win_score_config.get('required_fields', []):
                    fields_to_fetch.append(field)

            # TODO: make this return a dict, instead of list of dicts, splits will be fixed later
            if ck.startswith('live'):
                all_deals = retrieve_deals(model, ck, include_uip, node, fields=fields_to_fetch)
            else:
                all_deals = retrieve_deals(model, ck, include_uip, None, fields=fields_to_fetch)
            deal_results = {}
            relevant_deals = set(list(changed_deals) + opp_ids)
            if ck.startswith('live') and get_results_from_as_of:
                if relevant_deals:
                    for deal_id, deal_value in all_deals.items():
                        if deal_id in relevant_deals:
                            deal_results[deal_id] = deal_value
            else:
                deal_results = all_deals
            results[timestamp]['results'] = deal_results
            results[timestamp]['is_curr_q'] = is_curr_q
            results[timestamp]['timestamp'] = asof
            results[timestamp]['ck'] = ck
            results[timestamp]['results'] = transform_score(results[timestamp]['results'], win_score_config,
                                                            asof)  # to transform win scores
        if not ck.startswith('live'):
            if not if_exists:
                logger.info("Saving {} to daily-results bucket in s3".format(file_name))
                # gnana_storage.save_daily_results_to_s3(file_name, results[timestamp])

            if if_exists:
                logger.info("Reading {} from daily-results bucket in s3".format(file_name))
                results[timestamp] = gnana_storage.read_daily_results_from_s3(file_name)
            if fields or node:
                new_results = {}
                for deal in results[timestamp]['results']:
                    if fields and node:
                        if node in results[timestamp]['results'][deal].get('__segs'):
                            new_results[deal] = {}
                            for fld in fields:
                                if fld in results[timestamp]['results'][deal]:
                                    val = results[timestamp]['results'][deal][fld]
                                    if type(val) == dict:
                                        new_results[deal][fld] = val.get(node, val)
                                    else:
                                        new_results[deal][fld] = val
                    elif fields:
                        new_results[deal] = {}
                        for fld in fields:
                            if fld in results[timestamp]['results'][deal]:
                                new_results[deal][fld] = results[timestamp]['results'][deal][fld]
                    else:
                        if node in results[timestamp]['results'][deal].get('__segs'):
                            new_results[deal] = {}
                            for fld,val in results[timestamp]['results'][deal].items():
                                if type(val) == dict:
                                    new_results[deal][fld] = val.get(node, val)
                                else:
                                    new_results[deal][fld] = val
                results[timestamp]['results'] = new_results
    return results

