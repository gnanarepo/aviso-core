import logging
from urllib.parse import quote

from aviso.settings import sec_context

from deal_service.tasks import DealsTask
from infra import DEALS_COLL, NEW_DEALS_COLL
from infra.write import _remove_run_full_mode_flag
from tasks.hierarchy.hier_sync_tasks import Sync
from tasks.sync_drilldown import SyncDrilldown
from utils.date_utils import prev_periods_allowed_in_deals

logger = logging.getLogger('aviso-core.%s' % __name__)


class DrilldownSyncTask:

    def execute(self, period):
        try:
            sync_obj = SyncDrilldown(period=period)
            sync_obj.process()
            sync_obj.persist()
            result = sync_obj.return_value
            return {'success': True, 'result': result}
        except Exception as e:
            logger.exception(e)
            return {'success': False,
                    'error_msg': e}

class DrilldownSyncLeadTask:

    def execute(self, period):
        try:
            sync_obj = SyncDrilldown(period=period, service='leads')
            sync_obj.process()
            sync_obj.persist()
            result = sync_obj.return_value
            return {'success': True, 'result': result}
        except Exception as e:
            logger.exception(e)
            return {'success': False,
                    'error_msg': e}


class CaptureDrilldownsDiffsTask(DealsTask):

    def execute_forcefully(self):
        return True

    def execute(self, period, *args, **kwargs):
        self.period = period
        # fetch the result from gbm results
        new_deal_results = self.fetch_deal_results_from_gbm()
        new_opp_ids_and_nodes_drilldowns = {}
        new_opp_ids_and_nodes_hierachy = {}
        # prepare new drilldowns
        for p, data in  new_deal_results.iteritems():
            if not data:
                continue

            as_of = data['timestamp']

            for opp_id, deal in data['results'].iteritems():
                deal['opp_id'] = opp_id
                hierarchy_list, drilldown_list = self.adorn_hierarchy(deal, as_of)
                new_opp_ids_and_nodes_drilldowns[opp_id] = drilldown_list
                new_opp_ids_and_nodes_hierachy[opp_id] = hierarchy_list
        # fetch the current deals in deals collection
        old_deal_results = self.fetch_deals_from_deals_results()
        old_opp_ids_and_nodes_drilldowns = {}
        old_opp_ids_and_nodes_hierarchy = {}
        for res in old_deal_results:
            old_opp_ids_and_nodes_drilldowns[res['opp_id']] = res.get('drilldown_list', [])
            old_opp_ids_and_nodes_hierarchy[res['opp_id']] = res.get('hierarchy_list', [])

        # capture the impacted opp_ids
        impacted_opp_ids = []
        impacted_drilldowns = []
        for opp_id in new_opp_ids_and_nodes_drilldowns:
            new_drilldowns =  new_opp_ids_and_nodes_drilldowns[opp_id]
            old_drilldowns = None
            if opp_id in old_opp_ids_and_nodes_drilldowns:
                old_drilldowns = old_opp_ids_and_nodes_drilldowns[opp_id]
                diffs = [x for x in set(list(old_drilldowns + new_drilldowns)) if x not in new_drilldowns or x not in old_drilldowns]
                if diffs:
                    impacted_opp_ids.append(opp_id)
                impacted_drilldowns += diffs
        hierarchy_impacted_opp_ids = []
        for opp_id in new_opp_ids_and_nodes_hierachy:
            if opp_id in impacted_opp_ids:
                continue
            new_hierarchy =  new_opp_ids_and_nodes_hierachy[opp_id]
            old_hierarchy = None
            if opp_id in old_opp_ids_and_nodes_hierarchy:
                old_hierarchy = old_opp_ids_and_nodes_hierarchy[opp_id]
                diffs = [x for x in set(list(old_hierarchy + new_hierarchy)) if x not in new_hierarchy or x not in old_hierarchy]
                if diffs:
                    hierarchy_impacted_opp_ids.append(opp_id)
        if hierarchy_impacted_opp_ids:
            logger.warning("opp_ids impacted by hierarchy changes only %s" % hierarchy_impacted_opp_ids)
        _remove_run_full_mode_flag()
        return {'result': {'impacted_opp_ids': impacted_opp_ids,
                           'impacted_drilldowns': list(set(impacted_drilldowns)),
                           'hierarchy_impacted_opp_ids': hierarchy_impacted_opp_ids},
                'success': True}

    def fetch_deal_results_from_gbm(self):
        gbm_svc = sec_context.get_microservice_config('gbm_service')
        if not gbm_svc:
            logger.warning('no gbm service found')
            return {}
        fields = ['__segs']
        for owner_field, drilldown in self.config.owner_id_fields:
            if drilldown == 'CRR':
                continue  # Note: we are not using load_changes task for CRR
            fields.append(owner_field)
        field_params = "&".join(["=".join(["fields", quote(field)]) for field in fields])
        url = '/gbm/deals_results?period={}&{}'.format(self.period, field_params)
        gbm_shell = sec_context.gbm
        return gbm_shell.api(url, None)

    def fetch_deals_from_deals_results(self, db=None):
        deals_collection = db[DEALS_COLL] if db else sec_context.tenant_db[DEALS_COLL]
        prev_periods = prev_periods_allowed_in_deals()
        if self.period in prev_periods and self.config.config.get('update_new_collection'):
            deals_collection = sec_context.tenant_db[NEW_DEALS_COLL]
        project_fields = {'drilldown_list': 1, 'opp_id': 1, 'hierarchy_list': 1}
        all_deals = list(deals_collection.find({}, project_fields))
        return all_deals

class HierSyncLeadTask:

    def execute(self, period):
        try:
            sync_obj = Sync(period=period, service='leads')
            sync_obj.process()
            sync_obj.persist()
            result = sync_obj.return_value
            return {'success': True, 'result': result}
        except Exception as e:
            logger.exception(e)
            return {'success': False,
                    'error_msg': e}
