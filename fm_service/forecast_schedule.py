import copy
import logging

from aviso.settings import sec_context

from fm_service.fetch import FMFetch
from infra.read import fetch_ancestors, fetch_descendants
from infra.write import upload_forecast_schedule
from utils.mongo_reader import get_forecast_schedule

logger = logging.getLogger('gnana.%s' % __name__)
timezone_map = {'GMT': 'GMT', 'IST': 'Asia/Kolkata', 'PST': 'US/Pacific'}


class FMScheduleBase(FMFetch):

    def update_fm_schedule(self):
        self.node_descendants = {node for level in list(fetch_descendants(self.as_of,
                                                                          [self.node],
                                                                          levels=20,
                                                                          drilldown=True))[0]['descendants']
                                 for node in level.keys()}
        self.node_descendants.add(self.node)
        nodes = list(self.node_descendants)
        nodes.append('Global#!')
        nodes.append('Global#not_in_hier')
        fm_schedule = get_forecast_schedule(nodes, get_dict=True)
        new_nodes = self.nodes_having_changes

        records = []
        for node_ in list(set(new_nodes)):
            node_ancestors = fetch_ancestors(self.as_of, [node_])
            for ad in node_ancestors:
                if "ancestors" in ad and ad['ancestors']:
                    ancestors = ad['ancestors']
                    ancestors.reverse()
                    for ancestor in ancestors:
                        if ancestor in fm_schedule:
                            schedule = copy.deepcopy(fm_schedule[ancestor])
                            schedule['node_id'] = node_
                            schedule.pop("_id")
                            records.append(schedule)
                            break
        if records:
            upload_forecast_schedule(new_nodes, records)


class FMSchedule(FMScheduleBase):
    def __init__(self, period, nodes_having_changes,  *args, **kwargs):
        self.metrics = logger.new_metrics()
        self.debug = kwargs.get("debug", False)
        request = {'period': period,
                   'node': self.config.admin_node,
                   'nodes_having_changes': nodes_having_changes}
        self._get_metadata(request)
