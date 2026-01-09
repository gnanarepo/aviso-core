from collections import defaultdict
import logging

from aviso.utils import is_true


logger = logging.getLogger('gnana.%s' % __name__)
# List of created date buckets.
CD_BUCKET_KEYS = ['existing', 'new', 'total']


def rename_keys(ub_res):
    new_names = {'value': 'mean',
                 'up': 'upside',
                 'down': 'downside',
                 'existing_value': 'existing_mean',
                 'existing_up': 'existing_upside',
                 'existing_down': 'existing_downside',
                 }
    for name in new_names:
        if name in ub_res:
            ub_res[new_names[name]] = ub_res.pop(name)
    for key in ['mean', 'upside', 'downside']:
        if key not in ub_res:
            ub_res[key] = 0.0
    return

class CombineResults(object):

    @staticmethod
    def combine_results(results, combine_options, version='r'):

        # results: {opid:[views]}

        if results is not None:
            logger.info('len(results.keys()): %i', len(results.keys()))

        calc_qntls = is_true(combine_options.get('quantiles'))
        quantiles_corrections = combine_options.get("quantiles_corrections", False)
        ubf_res = combine_options.get('newpipeforecast', None)

        # TODO: this is a bit of janky way to tell which mode we're in, fix later
        from gbm_apis.deal_result.dataset import Dataset
        ds = Dataset.getByNameAndStage('OppDS', None)
        hs_impl = ds.params['general'].get('drilldown_config', {}).get('hs_impl', 'G')
        if hs_impl == 'A':
            logger.warning('Reformatting combined results to match old style')
            results = CombineResults.reformat_results(results)

        seg_value_opp = defaultdict(lambda: defaultdict(list))

        for ID, view_list in results.items():
            try:
                for view_idx, view in enumerate(view_list):
                    # new mode is list of nodes, old mode is seg dict thing
                    if isinstance(view['__segs'], dict):
                        for seg, seg_val in view['__segs'].items():
                            seg_value_opp[seg][seg_val].append((ID, view_idx))
                    else:
                        for node in view['__segs']:
                            seg_value_opp['!'][node].append((ID, view_idx))
            except Exception as e:
                logger.warning(f'id {ID}, view_list {view_list}')
                logger.warning(e)
                raise

        if len(seg_value_opp.keys()) == 0:
            logger.error("Heirarchy information not available!")

        # add segments which are not in epf but are in unborn
        if ubf_res:
            for ubf_seg, ubf_seg_vals in ubf_res.items():
                try:
                    seg_vals = seg_value_opp[ubf_seg]
                except KeyError:
                    logger.error("ERROR: unborn model had unexpected drilldown fields=%s", ubf_seg)
                    seg_vals = {}
                    seg_value_opp[ubf_seg] = seg_vals
                for ubf_seg_val in ubf_seg_vals:
                    if ubf_seg_val not in seg_vals:
                        seg_vals[ubf_seg_val] = []

        def make_crd(downside, forecast, upside):
            return {'downside': downside, 'mean': forecast, 'upside': upside}


        # [by_period] Still necessary, but shouldn't be called by_period anymore.
        def get_by_period_result():
            grand_result = {}
            from gbm_apis.deal_result.dataset import Dataset
            ds = Dataset.getByNameAndStage('OppDS', None)
            bands_config = ds.models['common'].config.get('bands_config', {})
            band_hi = bands_config.get('band_hi', 90)
            band_lo = bands_config.get('band_lo', 10)
            segment_config = ds.models['common'].config.get('segment_config', {})
            segment_forecast = segment_config.get('show_forecast', False)
            segment_forecast_using_open_pipe = segment_config.get('segment_forecast_using_ACV_open_pipe', False)
            non_rollup_segments = segment_config.get('non_rollup_segments', False)
            segment_amount = segment_config.get('amounts', {})
            excluded_segments = segment_config.get('excluded_segments', [])
            roll_up_segment_amount = segment_amount.copy()
            non_roll_up_segment_amount = {}
            for ele in excluded_segments:
                non_roll_up_segment_amount[ele] = roll_up_segment_amount.pop(ele)

            amount_fld = segment_config.get('amount_fld', "Amount_USD")
            terminal_fate_fld = segment_config.get('terminal_fate_fld', 'terminal_fate')
            logger.info('segment_forecast: '+str(segment_forecast))
            logger.info('segment_amount: '+str(segment_amount))
            logger.info('segmrnt terminal fate: '+str(terminal_fate_fld))
            if segment_forecast:
                logger.info('Segment forecast is enabled for the tenant. Combining results for segments')
            atds = Dataset.getByNameAndStage('AvisoTaskDS', None)
            bs_enabled = atds.params['general'].get('globals', {}).get('run_blackswan', False)
            if bs_enabled:
                logger.info('Black Swan is Enabled for the tenant. Combining Black Swan results')
            for seg in seg_value_opp:
                grand_result[seg] = {}
                for seg_val, view_id_list in seg_value_opp[seg].items():
                    ep_pred = 0.0
                    npd_pred = 0.0
                    bs_ep_pred = 0.0
                    bs_npd_pred = 0.0
                    tmpres = {}
                    for (ID, view_idx) in view_id_list:
                        view = results[ID][view_idx]
                        for res_fld, res_val in view.items():
                            try:
                                if res_fld.endswith('amount'):
                                    res_ctr_fld = res_fld[:-6] + 'count'
                                    tmpres[res_ctr_fld] = tmpres.get(res_ctr_fld, 0) + 1
                                    tmpres[res_fld] = tmpres.get(res_fld, 0.0) + float(res_val)
                            except Exception as e:
                                message = "ERROR: combining results for ID=%s. res_fld=%s, res_val=%s not floatable: %s"
                                logger.error(message, ID, res_fld, res_val, e)

                        # Note: In bookings version, if 'existing_pipe' exists,
                        # the deal is 'active'. In revenue, all deals have 'existing_pipe'
                        # key and we want to count them regardless of truth value.
                        if 'existing_pipe' not in view:
                            continue
                        win_prob = view['win_prob']
                        single_forecast = win_prob * view['eACV']
                        if bs_enabled:
                            bs_single_forecast = view['bs_forecast']
                        if view['existing_pipe']:
                            ep_pred += single_forecast
                            if bs_enabled:
                                bs_ep_pred += bs_single_forecast
                        else:
                            npd_pred += single_forecast
                            if bs_enabled:
                                bs_npd_pred += bs_single_forecast

                    dflt_qntl = 0.0 if calc_qntls else None
                    if bs_enabled:
                        bs_dflt_qntl = 0.0 if calc_qntls else None

                    res = {'new': {'forecast': make_crd(dflt_qntl, npd_pred, dflt_qntl)},
                           'existing': {'forecast': make_crd(dflt_qntl, ep_pred, dflt_qntl)},
                           'total': {'forecast': make_crd(dflt_qntl, ep_pred + npd_pred, dflt_qntl)}}
                    if bs_enabled:
                        res['new'].update({'bs_forecast': make_crd(bs_dflt_qntl, bs_npd_pred, bs_dflt_qntl)})
                        res['existing'].update({'bs_forecast': make_crd(bs_dflt_qntl, bs_ep_pred, bs_dflt_qntl)})
                        res['total'].update({'bs_forecast': make_crd(bs_dflt_qntl, bs_ep_pred + bs_npd_pred, bs_dflt_qntl)})

                    if ubf_res:
                        unborn_pre = ubf_res[seg].get(seg_val, {})
                        unborn_fcst = unborn_pre if unborn_pre else {'value': 0.0, 'up': 0.0, 'down': 0.0}
                        if atds.params['general'].get('globals', {}).get('ubf_mult'):
                            ubf_mult = atds.params['general'].get('globals', {})['ubf_mult']
                            for element in unborn_fcst.keys():
                                unborn_fcst[element] *= ubf_mult
                        rename_keys(unborn_fcst)
                        ub_pred = unborn_fcst['mean']
                        np_pred = npd_pred + ub_pred
                        tot_pred = ep_pred + np_pred
                        res['unborn'] = {'forecast': unborn_fcst}
                        res['total+unborn+won'] = {'forecast': make_crd(dflt_qntl, tot_pred, dflt_qntl)}
                        res['new+unborn+new_won'] = {'forecast': make_crd(dflt_qntl, np_pred, dflt_qntl)}
                        if bs_enabled:
                            bs_ubf_mult = atds.params['general'].get('globals', {}).get('bs_ubf_mult', 1.0)
                            bs_unborn_fcst = {a:b*bs_ubf_mult for a,b in unborn_fcst.items()}
                            bs_ub_pred = bs_unborn_fcst['mean']
                            bs_np_pred = bs_npd_pred + bs_ub_pred
                            bs_tot_pred = bs_ep_pred + bs_np_pred
                            res['unborn'].update({'bs_forecast': bs_unborn_fcst})
                            res['total+unborn+won'].update({'bs_forecast': make_crd(bs_dflt_qntl, bs_tot_pred, bs_dflt_qntl)})
                            res['new+unborn+new_won'].update({'bs_forecast': make_crd(bs_dflt_qntl, bs_np_pred, bs_dflt_qntl)})

                    # add segment level sum for existing pipe deals for each node and that for new pipe deals
                    # res['seg_ep_fcst'] = {'seg1': val1,
                    #                      'seg2': val2,
                    #                      ..........
                    #                      .......... }

                    # res['seg_ep_won'] = {'seg1': val1,
                    #                      'seg2': val2,
                    #                      ............
                    #                      ........... }

                    # res['seg_np_fcst'] = {'seg1': val1,
                    #                      'seg2': val2,
                    #                      ............
                    #                      ........... }

                    # res['seg_np_won'] = {'seg1': val1,
                    #                      'seg2': val2,
                    #                      ............
                    #                      ........... }

                    # res['seg_ubf_fcst'] = {'seg1': val1,
                    #                      'seg2': val2,
                    #                      ............
                    #                      ........... }
                    if segment_forecast:
                        for segment_key in ['seg_ep_fcst', 'seg_ep_won', 'seg_np_fcst', 'seg_np_won', 'seg_ubf_fcst']:
                            res[segment_key] = {segment: 0.0 for segment, segment_amt in segment_amount.items()}

                        if segment_forecast_using_open_pipe:
                            res['seg_ep_open_pipe'] = {segment: 0.0 for segment, segment_amt in segment_amount.items()}
                            res['seg_np_open_pipe'] = {segment: 0.0 for segment, segment_amt in segment_amount.items()}

                        for (ID, view_idx) in view_id_list:
                            view = results[ID][view_idx]
                            if 'existing_pipe' not in view:
                                continue
                            deal_forecast = view['forecast']
                            total_amt = view.get(amount_fld, 0.0)
                            try:
                                fc_ratio = float(deal_forecast) / total_amt
                            except:
                                fc_ratio = 0.0

                            # seg = '!'
                            # seg_val = 'Global#Global'
                            # segment_key == 'seg_ep_fcst'
                            # seg_fld = 'Initial Term'


                            if view['existing_pipe']:
                                #Calculating the open pipe of ep deals
                                if segment_forecast_using_open_pipe:
                                    if view.get('in_quarter', False):
                                        if view['win_prob'] not in {0, 1.0}:
                                            for segment, segment_amt in segment_amount.items():
                                                res['seg_ep_open_pipe'][segment] += sum([view.get(amount_fld, 0.0) for amt_fld in segment_amt if (view.get(amt_fld, 0.0) not in {'N/A', None, 0.0}) and (view.get(amount_fld, 0.0) not in {'N/A', None})])

                                for segment, segment_amt in roll_up_segment_amount.items():
                                    res['seg_ep_fcst'][segment] += fc_ratio * sum([view.get(amt_fld, 0.0) for amt_fld in segment_amt if view.get(amt_fld, 0.0) not in {'N/A', None}])
                                for segment, segment_amt in non_roll_up_segment_amount.items():
                                    res['seg_ep_fcst'][segment] += view['win_prob'] * sum([view.get(amt_fld, 0.0) for amt_fld in segment_amt if view.get(amt_fld, 0.0) not in {'N/A', None}])

                                # we are checking if total won_amount is non zero for filtering out the next quarter won deals
                                if view[terminal_fate_fld] == 'W' and view.get('won_amount', 0.0):
                                    for segment, segment_amt in segment_amount.items():
                                        res['seg_ep_won'][segment] += sum([view.get(amt_fld, 0.0) for amt_fld in segment_amt if view.get(amt_fld, 0.0) not in {'N/A', None}])
                            else:
                                # Calculating the open pipe of np deals
                                if segment_forecast_using_open_pipe:
                                    if view.get('in_quarter', False):
                                        if view['win_prob'] not in {0, 1.0}:
                                            for segment, segment_amt in segment_amount.items():
                                                res['seg_np_open_pipe'][segment] += sum([view.get(amount_fld, 0.0) for amt_fld in segment_amt if (view.get(amt_fld, 0.0) not in {'N/A', None, 0.0}) and (view.get(amount_fld, 0.0) not in {'N/A', None})])

                                for segment, segment_amt in roll_up_segment_amount.items():
                                    res['seg_np_fcst'][segment] += fc_ratio * sum([view.get(amt_fld, 0.0) for amt_fld in segment_amt if view.get(amt_fld, 0.0) not in {'N/A', None}])
                                for segment, segment_amt in non_roll_up_segment_amount.items():
                                    res['seg_np_fcst'][segment] += view['win_prob'] * sum([view.get(amt_fld, 0.0) for amt_fld in segment_amt if view.get(amt_fld, 0.0) not in {'N/A', None}])

                                # we are checking if total won_amount is non zero for filtering out the next quarter won deals
                                if view[terminal_fate_fld] == 'W' and view.get('won_amount', 0.0):
                                    for segment, segment_amt in segment_amount.items():
                                        res['seg_np_won'][segment] += sum([view.get(amt_fld, 0.0) for amt_fld in segment_amt if view.get(amt_fld, 0.0) not in {'N/A', None}])

                            for segment, segment_amt in roll_up_segment_amount.items():
                                res['seg_ubf_fcst'][segment] += fc_ratio * sum([view.get(amt_fld, 0.0) for amt_fld in segment_amt if view.get(amt_fld, 0.0) not in {'N/A', None}])
                            for segment, segment_amt in non_roll_up_segment_amount.items():
                                res['seg_ubf_fcst'][segment] += view['win_prob'] * sum([view.get(amt_fld, 0.0) for amt_fld in segment_amt if view.get(amt_fld, 0.0) not in {'N/A', None}])

                    for k, v in tmpres.items():
                        if k.startswith('new_pipe_'):
                            res['new'][k[9:]] = v
                        elif k.startswith('existing_pipe_'):
                            res['existing'][k[14:]] = v
                        else:
                            res['total'][k] = v

                    #convert into ratio
                    #TO-DO: see if we need to handle won amounts differently, i.e if the won_tot is zero then put zeros in ratio
                    if segment_forecast:
                        s_fields = ['seg_ep_fcst', 'seg_ep_won', 'seg_np_fcst', 'seg_np_won', 'seg_ubf_fcst']

                        if segment_forecast_using_open_pipe:
                            s_fields += ['seg_ep_open_pipe', 'seg_np_open_pipe']

                        for segment_key in s_fields:

                            """Creating a amount field to make use of this in future if ratio is zero because of won_amount is zero"""
                            segment_key_amount = segment_key+"_amount"
                            res[segment_key_amount] = res[segment_key].copy()

                            seg_amts = list(res[segment_key].values())
                            seg_amts_tot = float(sum(seg_amts))
                            if seg_amts_tot:
                                for seg_fld in res[segment_key]:
                                    if non_rollup_segments:
                                        if segment_key == 'seg_ep_won':
                                            """Division Bt Zero handled and divided the segment amount with existing pipe won amount"""
                                            res[segment_key][seg_fld] = res["existing"].get("won_amount", 0) and res[segment_key][seg_fld]/res["existing"].get("won_amount", 0) or 0
                                        elif segment_key == 'seg_np_won':
                                            """Division Bt Zero handled and divided the segment amount with new pipe won amount"""
                                            res[segment_key][seg_fld] = res["new"].get("won_amount", 0) and res[segment_key][seg_fld]/res["new"].get("won_amount", 0)  or 0
                                        elif segment_key == 'seg_ep_fcst':
                                            """Division Bt Zero handled and divided the segment amount with existing pipeline forecast mean"""
                                            res[segment_key][seg_fld] = res["existing"]["forecast"]["mean"]  and res[segment_key][seg_fld]/res["existing"]["forecast"]["mean"]  or 0
                                        elif segment_key == 'seg_np_fcst':
                                            """Division Bt Zero handled and divided the segment amount with new pipeline forecast mean"""
                                            res[segment_key][seg_fld] = res["new"]["forecast"]["mean"] and res[segment_key][seg_fld]/res["new"]["forecast"]["mean"]  or 0
                                        else:
                                            """Division Bt Zero handled and divided the segment amount with amount segment total"""
                                            res[segment_key][seg_fld] = seg_amts_tot  and res[segment_key][seg_fld]/seg_amts_tot  or 0
                                    else:
                                        """Division Bt Zero handled and divided the segment amount total with amount segment total"""
                                        res[segment_key][seg_fld] = seg_amts_tot and res[segment_key][seg_fld]/ seg_amts_tot or 0

                            else:
                                for seg_fld in res[segment_key]:
                                    res[segment_key][seg_fld] = len(seg_amts) and  1.0 / len(seg_amts) or 0

                    grand_result[seg][seg_val] = res

            #  calculate shifts due to already won
            tot_won = {}
            new_won = {}
            for seg in grand_result:
                tot_won[seg] = {}
                new_won[seg] = {}
                for seg_val, seg_val_dtls in grand_result[seg].items():
                    tot_won[seg][seg_val] = seg_val_dtls['total'].get('won_amount', 0.0)
                    new_won[seg][seg_val] = seg_val_dtls['new'].get('won_amount', 0.0)
                    if ubf_res and version == 'b':
                        seg_val_dtls["total+unborn+won"]["forecast"]["mean"] += tot_won[seg][seg_val]
                        seg_val_dtls["new+unborn+new_won"]["forecast"]["mean"] += new_won[seg][seg_val]
                        if bs_enabled:
                            seg_val_dtls["total+unborn+won"]["bs_forecast"]["mean"] += tot_won[seg][seg_val]
                            seg_val_dtls["new+unborn+new_won"]["bs_forecast"]["mean"] += new_won[seg][seg_val]

            if calc_qntls:
                opp_buckets = {cd_bucket: {} for cd_bucket in CD_BUCKET_KEYS}
                for ID, view_list in results.items():
                    for view_idx, view in enumerate(view_list):
                        # See comment above. This if never triggered in 'r' mode.
                        if 'existing_pipe' not in view:
                            continue
                        opp_buckets['total'][(ID, view_idx)] = view
                        n_or_e = 'existing' if view['existing_pipe'] else 'new'
                        opp_buckets[n_or_e][(ID, view_idx)] = view

                quantiles_num_paths = int(combine_options.get('num_paths', 20000))
                quantiles_random_seed = int(combine_options.get('random_seed', 7))
                extra_target_quantiles = eval(combine_options.get('extra_target_quantiles',
                                                                  '[40,60]'))
                quantiles_to_have = sorted(list(set(extra_target_quantiles) | {band_lo, band_hi}))

                from gbm.analyticengine.forecast_distribution import calc_paths_segments, \
                    calc_quantiles_segments, convolve_paths_and_unborn

                path_values = {}
                for cd_bucket in CD_BUCKET_KEYS:
                    if opp_buckets[cd_bucket]:
                        path_values[cd_bucket] = calc_paths_segments(opp_buckets[cd_bucket],
                                                                     quantiles_num_paths,
                                                                     quantiles_random_seed)
                        quantiles = calc_quantiles_segments(path_values[cd_bucket],
                                                            quantiles_to_have)
                        for seg, seg_vals in quantiles.items():
                            for seg_val, node_dtls in seg_vals.items():
                                grand_res_fcst = grand_result[seg][seg_val][cd_bucket]['forecast']
                                ep_mean = grand_res_fcst['mean']
                                lo = node_dtls[band_lo]
                                hi = node_dtls[band_hi]
                                if quantiles_corrections:
                                    lo = min(lo, ep_mean)
                                    hi = max(hi, ep_mean)
                                grand_res_fcst['downside'] = lo
                                grand_res_fcst['upside'] = hi

                                # TODO : Compute percentiles properly and not hard code it.
                                if bs_enabled:
                                    bs_grand_res_fcst = grand_result[seg][seg_val][cd_bucket]['bs_forecast']
                                    bs_ep_mean = bs_grand_res_fcst['mean']
                                    bs_grand_res_fcst['downside'] = bs_ep_mean * 0.85
                                    bs_grand_res_fcst['upside'] = bs_ep_mean * 1.15




                if ubf_res:
                    if "total" not in path_values:  # TODO: temporary debug statement
                        logger.error("ERROR: combined_results path_values is missing total key: %s", list(path_values.keys()))
                    total_and_unborn_path_values = convolve_paths_and_unborn(path_values['total'],
                                                                             ubf_res,
                                                                             quantiles_random_seed)
                    total_and_unborn_quantiles = calc_quantiles_segments(total_and_unborn_path_values,
                                                                         quantiles_to_have)

                    new_and_unborn_quantiles = {}
                    if opp_buckets['new']:
                        new_and_unborn_path_values = convolve_paths_and_unborn(path_values['new'],
                                                                               ubf_res,
                                                                               quantiles_random_seed)
                        new_and_unborn_quantiles = calc_quantiles_segments(new_and_unborn_path_values,
                                                                           quantiles_to_have)

                    for gr_key, quantiles, won_dict in [("total+unborn+won", total_and_unborn_quantiles, tot_won),
                                                        ("new+unborn+new_won", new_and_unborn_quantiles, new_won)]:
                        for seg, seg_vals in grand_result.items():
                            for seg_val, grand_res_dtls in seg_vals.items():
                                qtl_node_dtls = quantiles.get(seg, {}).get(seg_val, None)
                                if qtl_node_dtls:
                                    lo = qtl_node_dtls[band_lo]
                                    hi = qtl_node_dtls[band_hi]
                                else:
                                    ubf_node_dtls = ubf_res.get(seg, {}).get(seg_val, {})
                                    lo = ubf_node_dtls.get("downside", 0.0)
                                    hi = ubf_node_dtls.get("upside", 0.0)
                                node_won_amt = won_dict.get(seg, {}).get(seg_val, 0.0) if version == "b" else 0.0
                                lo += node_won_amt
                                hi += node_won_amt
                                grand_res_fcst = grand_res_dtls[gr_key]["forecast"]
                                grand_fcst_mean = grand_res_fcst["mean"]
                                if quantiles_corrections:
                                    lo = min(lo, grand_fcst_mean)
                                    hi = max(hi, grand_fcst_mean)
                                grand_res_fcst["downside"] = lo
                                grand_res_fcst["upside"] = hi

                                # TODO : Compute percentiles properly and not hard code it.
                                if bs_enabled:
                                    bs_grand_res_fcst = grand_res_dtls[gr_key]["bs_forecast"]
                                    bs_grand_fcst_mean = bs_grand_res_fcst["mean"]
                                    bs_grand_res_fcst["downside"] = bs_grand_fcst_mean * 0.85
                                    bs_grand_res_fcst["upside"] = bs_grand_fcst_mean * 1.15


            return grand_result

        merged_result = get_by_period_result()

        return merged_result

    @staticmethod
    def reformat_results(results):
        '''
        the new zd format would be rough to rewrite all this for,
        so for now, we'll just turn out new format back into a single
        view for each seg, make life easier
        results = {
           'oppid': {
              unsplit_fld: unsplit_val,
              split_fld: {
                  node_id: split_val,
                  node_id: split_val,
              }}}
        '''
        # yolo try except, but maybe this later
        ## we're assuming every split field has a value for every seg, which is fair i think
        #first_vals = next(iter(results.values()))
        #split_flds = []
        #first_seg = first_vals['__segs'][0]
        #for fld, vals in first_vals.items():
        #    if not isinstance(vals, dict):
        #        continue
        #    if not first_seg in vals:
        #        continue
        #    split_flds.append(fld)

        output = {}
        # then we'll iterate over every opp
        for oppid, opp_dict in results.items():
            view_list = []
            for seg in opp_dict['__segs']:
                view_dict = {'__segs': [seg]}
                for fld, fld_val in opp_dict.items():
                    if fld == '__segs':
                        continue
                    try:
                        view_dict[fld] = fld_val[seg]
                    except:
                        view_dict[fld] = fld_val
                view_list.append(view_dict)
            output[oppid] = view_list
        return output
