import logging

from copy import deepcopy

from aviso.utils.dateUtils import get_prevq_mnem

from utils.date_utils import current_period, xl2datetime

logger = logging.getLogger('gnana.%s' % __name__)


def generate_subscription_same_dd_recs(rev_period, drilldown, close_date_fld, results, filt_config):
    '''
    Subscription records are generated when the customer has subscription starting date and its amount associated with a deal.
    Example tenant - appannie.com

    Rules:
    1. Look for subscriptions date and create dummy deals if termtype is churn or renewal, for these
    deals put only the drilldown which is configured for dummy deals
    2. For originial deals if the termtype is churn or renewal then remove the dummy deals drill down
    3. On top of rule 2, for a deal if a- the termtype is churn/renewal, b- deal is closed won and c- deal has
    UpsellACV, create an original deal record with original close date.



    Config:

    this_shell.gbm.dataset(dataset='OppDS')['models']['common']['config']['rev_schedule_config']

    {
        u'close_date_fld': u'as_of_CloseDate_adj',
        u'drilldown': [u'Global', 'Type'],  # this can be configured as single drilldown in str format
        u'filt_config': {u'conditional_field': u'as_of_TermType',
        u'conditional_field_val': [u'Churn', u'Renewal']},
        u'prd_rev_schedule': True,
        u'prd_subs_schedule_same_dd': True,
        u'rev_schedule_field': u'RevSchedule',
        u'rev_schedule_terminal_date_fld': u'rs_terminal_date'
        }
    '''

    conditional_field = filt_config.get('conditional_field', 'as_of_TermType')
    conditional_field_val = filt_config.get('conditional_field_val', ['Churn', 'Renewal'])
    terminal_fate_fld = filt_config.get('terminal_fate_fld', 'as_of_terminal_fate')
    required_amt_fld = filt_config.get('required_amt_fld', 'as_of_UpsellACV')
    ret_val = {}
    current_period_mne = current_period().mnemonic
    for opp_id, res in results.items():
        deal_rev_sched_amount = res.get('as_of_raw_rev_schedule_amounts', {})
        deal_rev_sched_date = res.get('as_of_raw_rev_schedule_dates', {})
        deal_close_date = res.get(close_date_fld, 'N/A')
        deal_conditional_field_val = res.get(conditional_field, 'N/A')

        # if close date is "N/A" then just return the records
        if deal_close_date == 'N/A':
            ret_val[opp_id] = dict(res)
            if deal_conditional_field_val in conditional_field_val:
                if not (ret_val[opp_id][terminal_fate_fld] == 'W' and ret_val[opp_id].get(required_amt_fld, 0.0) > 0.0):
                    ret_val[opp_id]['__segs'] = [seg for seg in ret_val[opp_id]['__segs'] if
                                                 not seg.split("#")[0] in drilldown]

                for mne, amt in deal_rev_sched_amount.items():
                    dummy_id = opp_id + '_' + mne
                    ret_val[dummy_id] = dict(res)
                    ret_val[dummy_id]['rev_schedule_amount'] = amt
                    ret_val[dummy_id][close_date_fld] = deal_rev_sched_date.get(mne, 'N/A')
                    ret_val[dummy_id]['__segs'] = [seg for seg in ret_val[dummy_id]['__segs'] if
                                                   seg.split("#")[0] in drilldown]
                    ret_val[dummy_id]['__dummy_deal_rec'] = True

            continue

        deal_close_date_prd = current_period(a_datetime=xl2datetime(deal_close_date)).mnemonic
        # when rev_period is in past
        if rev_period < current_period_mne:
            ret_val[opp_id] = dict(res)
            ret_val[opp_id]['rev_schedule_amount'] = deal_rev_sched_amount.get(rev_period, 0.0)
            if deal_conditional_field_val in conditional_field_val:
                # do not remove configured drilldown if deal is open and deal has perticular amount
                if not (ret_val[opp_id][terminal_fate_fld] == 'W' and ret_val[opp_id].get(required_amt_fld, 0.0) > 0.0):
                    ret_val[opp_id]['__segs'] = [seg for seg in ret_val[opp_id]['__segs'] if
                                                 not seg.split("#")[0] in drilldown]

                if rev_period in deal_rev_sched_amount:
                    dummy_id = opp_id + '_' + rev_period
                    ret_val[dummy_id] = dict(res)
                    ret_val[dummy_id]['rev_schedule_amount'] = deal_rev_sched_amount.get(rev_period, 0.0)
                    ret_val[dummy_id][close_date_fld] = deal_rev_sched_date.get(rev_period, 'N/A')
                    ret_val[dummy_id]['__segs'] = [seg for seg in ret_val[dummy_id]['__segs'] if
                                                   seg.split("#")[0] in drilldown]
                    ret_val[dummy_id]['__dummy_deal_rec'] = True

        # when rev_period is current period
        else:
            # the deals with close date period in past would be ignored in caches run on forecast app end
            ret_val[opp_id] = dict(res)
            # for original deal record, revenue schedule amount should be taken from deal close date period
            ret_val[opp_id]['rev_schedule_amount'] = deal_rev_sched_amount.get(deal_close_date_prd, 0.0)
            if deal_conditional_field_val in conditional_field_val:
                # do not remove configured drilldown if deal is open and deal has perticular amount
                if not (ret_val[opp_id][terminal_fate_fld] == 'W' and ret_val[opp_id].get(required_amt_fld, 0.0) > 0.0):
                    ret_val[opp_id]['__segs'] = [seg for seg in ret_val[opp_id]['__segs'] if
                                                 not seg.split("#")[0] in drilldown]

                # create record for every remaining revenue period
                for mne, amt in deal_rev_sched_amount.items():
                    if mne >= current_period_mne:
                        dummy_id = opp_id + '_' + mne
                        ret_val[dummy_id] = dict(res)
                        ret_val[dummy_id]['rev_schedule_amount'] = amt
                        ret_val[dummy_id][close_date_fld] = deal_rev_sched_date.get(mne, 'N/A')
                        ret_val[dummy_id]['__segs'] = [seg for seg in ret_val[dummy_id]['__segs'] if
                                                       seg.split("#")[0] in drilldown]
                        ret_val[dummy_id]['__dummy_deal_rec'] = True

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
    subscription_date_amounts = rev_schedule_config.get('subscription_date_amounts',
                                                        {'as_of_Amount_USD': u'lambda x : True'})
    delayed_renewal_amounts = rev_schedule_config.get('delayed_renewal_amounts',
                                                      {'as_of_Amount_USD': u'lambda x : True'})

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
        deal_subscription_date_month = current_period(a_datetime=xl2datetime(deal_subscription_date),
                                                      period_type='M').mnemonic
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

    if not (res.get(renewal_amount_fld, 0) and res.get(upsell_amount_fld, 0)) and \
            deal_close_date_month == deal_subscription_date_month:
        ret_val[opp_id] = deepcopy(res)
        for fld in delayed_renewal_amount_list:
            ret_val[opp_id][fld] = 0.0
    else:
        close_date_amounts_list = [fld for fld, dtls in close_date_amounts.items() if res[fld] and eval(dtls)(res)]
        subscription_date_amount_list = [fld for fld, dtls in subscription_date_amounts.items() if
                                         res[fld] and eval(dtls)(res)]
        try:
            close_date_amounts = {fld: float(res[fld]) for fld in close_date_amounts_list}
        except:
            logger.info('Appannie dealID:' + str(opp_id) + '. Exception while computing close_date_amounts')
            close_date_amounts = {fld: float(res[fld]) if res[fld] != 'N/A' else 0.0 for fld in close_date_amounts_list}
        try:
            subscription_date_amounts = {fld: float(res[fld]) for fld in subscription_date_amount_list}
        except:
            logger.info('Appannie dealID:' + str(opp_id) + '. Exception while computing subscription_date_amounts')
            subscription_date_amounts = {fld: float(res[fld]) if res[fld] != 'N/A' else 0.0 for fld in
                                         subscription_date_amount_list}
        close_date_amt_sum = sum([amt for fld, amt in close_date_amounts.items() if fld in total_amount_components])
        subscription_date_amt_sum = sum(
            [amt for fld, amt in subscription_date_amounts.items() if fld in total_amount_components])
        total_amount = float(res[total_amount_fld])
        if deal_close_date_prd != 'N/A' and deal_close_date_prd >= rev_period:
            ret_val[opp_id] = deepcopy(res)
            if total_amount:
                ratio = close_date_amt_sum / total_amount
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
            dummy_id = opp_id + '_' + deal_subscription_date_prd
            ret_val[dummy_id] = deepcopy(res)
            for fld in delayed_renewal_amount_list:
                if opp_id in ret_val:  # Quick Fix for the issue. Need to confirm Delayed Renewal Logic later.
                    ret_val[opp_id][fld] = 0.0
                # ret_val[dummy_id][fld] = 0.0 # This should be the line instead of above two lines. Need to confirm.
            ret_val[dummy_id][close_date_fld] = deal_subscription_date
            if total_amount:
                ratio = subscription_date_amt_sum / total_amount
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
            ret_val[opp_id]['__segs'] = [seg for seg in ret_val[opp_id]['__segs'] if
                                         not seg.startswith(drilldown + '#')]

            for mne, amt in deal_rev_sched_amount.items():
                dummy_id = opp_id + '_' + mne
                ret_val[dummy_id] = dict(res)
                ret_val[dummy_id]['rev_schedule_amount'] = amt
                ret_val[dummy_id][close_date_fld] = deal_rev_sched_date.get(mne, 'N/A')
                ret_val[dummy_id]['__segs'] = [seg for seg in ret_val[dummy_id]['__segs'] if
                                               seg.startswith(drilldown + '#')]

            continue

        deal_close_date_prd = current_period(a_datetime=xl2datetime(deal_close_date)).mnemonic
        # when rev_period is in past
        if rev_period < current_period_mne:
            ret_val[opp_id] = dict(res)
            ret_val[opp_id]['rev_schedule_amount'] = deal_rev_sched_amount.get(rev_period, 0.0)
            ret_val[opp_id]['__segs'] = [seg for seg in ret_val[opp_id]['__segs'] if
                                         not seg.startswith(drilldown + '#')]

            if rev_period in deal_rev_sched_amount:
                dummy_id = opp_id + '_' + rev_period
                ret_val[dummy_id] = dict(res)
                ret_val[dummy_id]['rev_schedule_amount'] = deal_rev_sched_amount.get(rev_period, 0.0)
                ret_val[dummy_id][close_date_fld] = deal_rev_sched_date.get(rev_period, 'N/A')
                ret_val[dummy_id]['__segs'] = [seg for seg in ret_val[dummy_id]['__segs'] if
                                               seg.startswith(drilldown + '#')]

        # when rev_period is current period
        else:
            # the deals with close date period in past would be ignored in caches run on forecast app end
            ret_val[opp_id] = dict(res)
            # for original deal record, revenue schedule amount should be taken from deal close date period
            ret_val[opp_id]['rev_schedule_amount'] = deal_rev_sched_amount.get(deal_close_date_prd, 0.0)
            ret_val[opp_id]['__segs'] = [seg for seg in ret_val[opp_id]['__segs'] if
                                         not seg.startswith(drilldown + '#')]

            # create record for every remaining revenue period
            for mne, amt in deal_rev_sched_amount.items():
                if mne >= current_period_mne:
                    dummy_id = opp_id + '_' + mne
                    ret_val[dummy_id] = dict(res)
                    ret_val[dummy_id]['rev_schedule_amount'] = amt
                    ret_val[dummy_id][close_date_fld] = deal_rev_sched_date.get(mne, 'N/A')
                    ret_val[dummy_id]['__segs'] = [seg for seg in ret_val[dummy_id]['__segs'] if
                                                   seg.startswith(drilldown + '#')]

    return ret_val


def generate_expiration_date_renewal_rec(rev_period, renewal_drilldown, close_date_fld, expiration_date_fld, type_fld,
                                         renewal_vals, opp_id, res, splitted_fields):
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
        ret_val[opp_id]['__segs'] = [element for element in ret_val[opp_id]['__segs'] if
                                     not element.startswith(renewal_drilldown + '#')]
        for fld in dict_flds:
            if fld in ret_val[opp_id]:
                for element in list(ret_val[opp_id][fld]):
                    if element.startswith(renewal_drilldown + '#'):
                        del (ret_val[opp_id][fld][element])
    if deal_type in renewal_vals:
        dummy_id = opp_id + '_renewal'
        ret_val[dummy_id] = deepcopy(res)
        ret_val[dummy_id]['__segs'] = [element for element in ret_val[dummy_id]['__segs'] if
                                       element.startswith(renewal_drilldown + '#')]
        for fld in dict_flds:
            if fld in ret_val[dummy_id]:
                for element in list(ret_val[dummy_id][fld]):
                    if not element.startswith(renewal_drilldown + '#'):
                        del (ret_val[dummy_id][fld][element])
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

    #################################################

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
            # or condition is for case where current prd is close dt period but products are in next period
            if (mne >= current_period_mne and deal_close_date_prd != current_period_mne) or \
                    (
                            mne > current_period_mne):  # we don't need to create dummy record if deal_close_date_prd = current_period_mne as revenue scheduling is handled above for that case
                dummy_id = opp_id + '_' + mne
                ret_val[dummy_id] = dict(res)
                ret_val[dummy_id]['rev_schedule_amount'] = sum([float(ele['Amount']) for ele in amt_list])
                ret_val[dummy_id][total_amount_fld] = ret_val[dummy_id]['rev_schedule_amount']
                ret_val[dummy_id][close_date_fld] = deal_rev_sched_date.get(mne, 'N/A')
                ret_val[dummy_id]['__segs'] = [seg for seg in ret_val[dummy_id]['__segs'] if
                                               seg.startswith(drilldown + '#')]
                ret_val[dummy_id][rev_schedule_config['product_list_field']] = amt_list

                logger.info('Github dealID:' + str(dummy_id) + 'mne:' + str(mne) + 'current_period_mne:' + str(
                    current_period_mne))
                for product in amt_list:
                    ret_val[dummy_id][seg_amounts_dict.get(product['Segment'])[0]] += product['Amount']
                if mne > current_period_mne:  # making won_amount as 0 for future quarter dummy deals
                    ret_val[dummy_id]['won_amount'] = {seg: 0 for seg in revenue_segs}
                else:
                    ret_val[dummy_id]['won_amount'] = {seg: ret_val[dummy_id]['rev_schedule_amount'] for seg in
                                                       revenue_segs}
    return ret_val


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
                dummy_id = opp_id + '_' + rev_period
                ret_val[dummy_id] = dict(res)
                ret_val[dummy_id]['rev_schedule_amount'] = deal_rev_sched_amount.get(rev_period, 0.0)
                ret_val[dummy_id][close_date_fld] = deal_rev_sched_date.get(rev_period, 'N/A')
                ret_val[dummy_id]['__segs'] = [seg for seg in ret_val[dummy_id]['__segs'] if
                                               seg.startswith(drilldown + '#')]
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
                    dummy_id = opp_id + '_' + mne
                    ret_val[dummy_id] = dict(res)
                    ret_val[dummy_id]['rev_schedule_amount'] = amt
                    ret_val[dummy_id][close_date_fld] = deal_rev_sched_date.get(mne, 'N/A')
                    ret_val[dummy_id]['__segs'] = [seg for seg in ret_val[dummy_id]['__segs'] if
                                                   seg.startswith(drilldown + '#')]

    return ret_val


def add_prefix(aFeature, default_prefix='latest_'):
    # Eswar claims this optimization gives a 5% performance improvement in model runtime.
    # Be careful to choose prefixes that don't invalidate this optimization.
    if aFeature[0] == 'a':
        if (aFeature[:9] == 'as_of_fv_' or
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
