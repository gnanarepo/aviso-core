import copy
import datetime
import json
import logging
from collections import OrderedDict

import pytz
from aviso.settings import sec_context

from fm_service.fetch import FMFetch
from infra.read import (fetch_ancestors, fetch_children, fetch_descendants,
                        fetch_node_to_parent_mapping_and_labels,
                        get_period_begin_end)
from infra.write import upload_forecast_schedule
from utils.date_utils import epoch
from utils.mongo_reader import get_forecast_schedule

logger = logging.getLogger('gnana.%s' % __name__)
timezone_map = {'GMT': 'GMT', 'IST': 'Asia/Kolkata', 'PST': 'US/Pacific'}


class FMSchedule(FMFetch):
    """
    fetch fm schedule for a node and its direct children

    Parameters:
        node -- hierarchy node id                                         0050000FLN2C9I2

    Returns:
        dict -- fm schedule
    # "Global#Global": {
#        "heading": "Global",
#        "forecastWindow": "lock", //unlock || lock
#        "forecastTimestamp": "1723672225549",
#        "recurring": true, // true or false
#        "unlockPeriod": "month", //month or quarter
#        "unlockFreq": "month", // month, dats or weekdays
#        "unlockDay": 1, // Between 1-31
#        "unlocktime": "21:00:00",
#        "lockPeriod": "month", //month or quarter
#        "lockFreq": "month", // month, dats or weekdays
#        "lockDay": 1, // Between 1-31
#        "locktime": "21:00:00",
#        "timeZone": "pst"
#     }
    """
    http_method_names = ['get', 'post']

    def get(self, request, *args, **kwargs):
        super(FMSchedule, self).get(request, *args, **kwargs)
        self.admin_node = self.config.admin_node
        descendants = [(rec['node'], rec['descendants'][0], rec['descendants'][1])
                       for rec in
                       fetch_descendants(self.as_of, [self.node], levels=2, include_children=True, drilldown=True,
                                         period=self.period, boundary_dates=self.boundary_dates)]

        node_children_check = {node: any(node_children) for node, node_children, _ in descendants}
        nodes = node_children_check.keys()
        timestamp = None
        if self.is_versioned:
            timestamp, _ = get_period_begin_end(self.period)
        _, labels = fetch_node_to_parent_mapping_and_labels(timestamp,
                                                            drilldown=True,
                                                            period=self.period,
                                                            boundary_dates=self.boundary_dates)
        fm_schedule = get_forecast_schedule(nodes)
        as_of = epoch().as_epoch()
        schedules = {}
        if not fm_schedule:
            for node in nodes:
                heading = labels[node]
                schedules[node] = {
                    "heading": heading,
                    "forecastWindow": self.config.forecast_window_default,
                    "forecastTimestamp": as_of,
                    "recurring": False}
        records = []
        timeZone = 'US/Pacific'
        for sch in fm_schedule:
            heading = labels[sch['node_id']]
            node_id = sch['node_id']
            recurring = sch['recurring']
            lockPeriod = sch.get('lockPeriod', 'month')
            lockFreq = sch.get('lockFreq', 'month')
            unlockfreq = sch.get('unlockFreq', 'month')
            unlockDay = int(sch.get('unlockDay', 0))
            unlocktime = sch['unlocktime'].split(":") if 'unlocktime' in sch else ["00", "00"]
            lockDay = int(sch['lockDay']) if 'lockDay' in sch else 0
            locktime = sch['locktime'].split(":") if 'locktime' in sch else ["00", "00"]
            timeZone_original = sch['timeZone'] if 'timeZone' in sch else "PST"
            timeZone = timezone_map[timeZone_original.upper()]
            lock_on_saved = sch['lock_on'] if 'lock_on' in sch else None
            unlock_on_saved = sch['unlock_on'] if 'unlock_on' in sch else None

            if sch['recurring']:
                lock_on = None
                unlock_on = None
                current_year = datetime.datetime.now().year
                current_month = datetime.datetime.now().month
                if lockPeriod == 'month' or (lockPeriod == 'quarter' and lock_on is None):
                    unlock_on = datetime.datetime(current_year, current_month, unlockDay,
                                         int(unlocktime[0]), int(unlocktime[1]), 0)
                    zone = pytz.timezone(timeZone)
                    unlock_on = zone.localize(unlock_on)
                    if lockFreq == 'month':
                        lock_on = datetime.datetime(current_year, current_month, lockDay,
                                                    int(locktime[0]), int(locktime[1]), 0)
                        zone = pytz.timezone(timeZone)
                        lock_on = zone.localize(lock_on)
                    elif lockFreq == 'days' or lockFreq == 'weekdays':
                        lock_on = unlock_on + datetime.timedelta(lockDay)
                forecastWindow = "unlock"
                forecastTimestamp = epoch(unlock_on).as_epoch() if unlock_on else None
                if as_of >= epoch(lock_on).as_epoch():
                    forecastWindow = "lock"
                    forecastTimestamp = epoch(lock_on).as_epoch()
                if as_of >= epoch(unlock_on).as_epoch() and epoch(unlock_on).as_epoch() > epoch(lock_on).as_epoch():
                    forecastWindow = "unlock"
                    forecastTimestamp = epoch(unlock_on).as_epoch()

                schedules[sch['node_id']] = {
                    "heading": heading,
                    "forecastWindow": forecastWindow,
                    "forecastTimestamp": forecastTimestamp,
                    "recurring": recurring,
                    "lockPeriod": lockPeriod,
                    "unlockFreq": unlockfreq,
                    "unlockDay": unlockDay,
                    "unlocktime": (":").join(unlocktime) if isinstance(unlocktime, list) else unlocktime,
                    "lockPeriod": lockPeriod,
                    "lockFreq": lockFreq,
                    "lockDay": lockDay,
                    "locktime": (":").join(locktime) if isinstance(locktime, list) else locktime,
                    "timeZone": timeZone_original
                }
                record_to_update = {}
                if lock_on is not None and lock_on_saved != epoch(lock_on).as_epoch():
                    record_to_update['node_id'] = node_id
                    record_to_update['lock_on'] = epoch(lock_on).as_epoch()
                if unlock_on is not None and unlock_on_saved != epoch(unlock_on).as_epoch():
                    record_to_update['node_id'] = node_id
                    record_to_update['unlock_on'] = epoch(unlock_on).as_epoch()
                if record_to_update:
                    records.append(record_to_update)
            else:
                schedules[sch['node_id']] = {
                    "heading": heading,
                    "forecastWindow": sch['status_non_recurring'],
                    "forecastTimestamp": sch['non_recurring_timestamp'],
                    "recurring": recurring,
                    "lockPeriod": lockPeriod,
                    "unlockFreq": unlockfreq,
                    "unlockDay": unlockDay,
                    "unlocktime": (":").join(unlocktime) if isinstance(unlocktime, list) else unlocktime,
                    "lockPeriod": lockPeriod,
                    "lockFreq": lockFreq,
                    "lockDay": lockDay,
                    "locktime": (":").join(locktime) if isinstance(locktime, list) else locktime,
                    "timeZone": timeZone_original
                }
        child_order = [x['node'] for x in fetch_children(self.as_of, [self.node])]
        schedules = self.add_admin_forecastwindow(nodes, schedules, timeZone)
        schedules_ordered = OrderedDict()
        for ordered_node in [self.node] + child_order:
            if ordered_node in schedules:
                schedules_ordered[ordered_node] = schedules[ordered_node]
            else:
                schedules_for_node = copy.deepcopy(schedules[self.node])
                schedules_ordered[ordered_node] = schedules_for_node
                schedules_for_node['node_id'] = ordered_node
                records.append(schedules_for_node)
        if records:
            upload_forecast_schedule(nodes, records)
        return schedules_ordered


    def add_admin_forecastwindow(self, nodes, schedules, timeZone):
        if self.admin_node in nodes:
            admin_node_schedule = schedules.get(self.admin_node, {})
            admin_node_forecastwindow = admin_node_schedule.get('forecastWindow', self.config.forecast_window_default)
            for node, details in schedules.items():
                schedules[node]['admin_forecastWindow'] = admin_node_forecastwindow
        else:
            admin_node_schedule = get_forecast_schedule([self.admin_node])
            forecastWindow = self.config.forecast_window_default
            for sch in admin_node_schedule:
                as_of = epoch().as_epoch()
                if sch.get('recurring'):
                    lockPeriod = sch.get('lockPeriod', 'month')
                    lockFreq = sch.get('lockFreq', 'month')
                    unlockDay = int(sch.get('unlockDay', 0))
                    unlocktime = sch['unlocktime'].split(":") if 'unlocktime' in sch else ["00", "00"]
                    lockDay = int(sch['lockDay']) if 'lockDay' in sch else 0
                    locktime = sch['locktime'].split(":") if 'locktime' in sch else ["00", "00"]
                    timeZone_original = sch['timeZone'] if 'timeZone' in sch else "PST"
                    timeZone = timezone_map[timeZone_original.upper()]
                    lock_on = None
                    unlock_on = None
                    current_year = datetime.datetime.now().year
                    current_month = datetime.datetime.now().month
                    if lockPeriod == 'month' or (lockPeriod == 'quarter' and lock_on is None):
                        unlock_on = datetime.datetime(current_year, current_month, unlockDay,
                                                      int(unlocktime[0]), int(unlocktime[1]), 0)
                        zone = pytz.timezone(timeZone)
                        unlock_on = zone.localize(unlock_on)
                        if lockFreq == 'month':
                            lock_on = datetime.datetime(current_year, current_month, lockDay,
                                                        int(locktime[0]), int(locktime[1]), 0)
                            zone = pytz.timezone(timeZone)
                            unlock_on = zone.localize(lock_on)

                        elif lockFreq == 'days' or lockFreq == 'weekdays':
                            lock_on = unlock_on + datetime.timedelta(lockDay)
                    forecastWindow = "unlock"
                    if as_of >= epoch(lock_on).as_epoch():
                        forecastWindow = "lock"
                    if as_of >= epoch(unlock_on).as_epoch() and epoch(unlock_on).as_epoch() > epoch(lock_on).as_epoch():
                        forecastWindow = "unlock"
                else:
                    forecastWindow = sch.get('status_non_recurring', forecastWindow)
            for node, details in schedules.items():
                schedules[node]['admin_forecastWindow'] = forecastWindow
        return schedules

    def post(self, request, *args, **kwargs):
        super(FMSchedule, self).post(request, *args, **kwargs)
        post_data = json.loads(request.read())
        recurring = post_data.get('recurring')
        current_username = sec_context.get_effective_user().username
        timestamp = None
        if recurring:
            unlockFreq = post_data.get('unlockFreq')
            unlockDay = int(post_data.get('unlockDay'))
            unlocktime_original = post_data.get('unlocktime')
            unlocktime = post_data.get('unlocktime').split(":")
            lockPeriod = post_data.get('lockPeriod')
            unlockPeriod = post_data.get('unlockPeriod', lockPeriod)
            lockFreq = post_data.get('lockFreq')
            lockDay = int(post_data.get('lockDay'))
            locktime_original = post_data.get('locktime')
            locktime = post_data.get('locktime').split(":")
            timeZone_original = 'PST'
            timeZone = "US/Pacific"
            if 'timeZone' in post_data and post_data['timeZone'] in timezone_map:
                timeZone = timezone_map[post_data['timeZone'].upper()]
                timeZone_original = post_data['timeZone']

        else:
            status = post_data.get('status', None)
            timestamp = post_data.get("forecastTimestamp", None)
        nodes = [self.node]
        self.node_descendants = {node for level in list(fetch_descendants(self.as_of,
                                                                          [self.node],
                                                                          levels=20,
                                                                          drilldown=True))[0]['descendants']
                                 for node in level.keys()}
        self.node_descendants.add(self.node)
        nodes = self.node_descendants
        nodes.add('Global#!')
        nodes.add('Global#not_in_hier')
        records = []
        if recurring:
            lock_on = None
            unlock_on = None
            current_year = datetime.datetime.now().year
            current_month = datetime.datetime.now().month
            if lockPeriod == 'month' or (lockPeriod == 'quarter' and lock_on is None):
                unlock_on = datetime.datetime(current_year, current_month, unlockDay,
                                              int(unlocktime[0]), int(unlocktime[1]), 0)
                zone = pytz.timezone(timeZone)
                unlock_on = zone.localize(unlock_on)
                if lockFreq == 'month':
                    lock_on = datetime.datetime(current_year, current_month, lockDay,
                                                int(locktime[0]), int(locktime[1]), 0)
                    zone = pytz.timezone(timeZone)
                    lock_on = zone.localize(lock_on)
                elif lockFreq == 'days' or lockFreq == 'weekdays':
                    lock_on = unlock_on + datetime.timedelta(lockDay)
            for node in nodes:
                records.append({"recurring": recurring,
                                "unlockPeriod": unlockPeriod,
                                "unlockFreq": unlockFreq,
                                "unlockDay": unlockDay,
                                "unlocktime": unlocktime_original,
                                "lockPeriod": lockPeriod,
                                "lockFreq": lockFreq,
                                "lockDay": lockDay,
                                "locktime": locktime_original,
                                "timeZone": timeZone_original,
                                "node_id": node,
                                "lock_on": epoch(lock_on).as_epoch(),
                                "unlock_on": epoch(unlock_on).as_epoch()
                                })
        else:
            timestamp = timestamp if timestamp is not None else epoch().as_epoch()
            for node in nodes:
                records.append({"status_non_recurring": status,
                                "non_recurring_timestamp": timestamp,
                                "recurring": False,
                                "node_id": node})
            user_list = self.get_user_list()
            msg = "Forecast window is {}ed by your admin to fill - up the forecast data".format(status)
            from infra import send_notifications
            notif = []
            user_list_final = {}
            for username, u in user_list.items():
                if username == current_username:
                    continue
                user_nodes_ = []
                try:
                    role = u['roles'].get('user', None)
                    if role:
                        for i in role:
                            if i[0] == 'results':
                                if i[1] != '!':
                                    user_nodes_.append(i[1])
                except:
                    pass
                user_nodes_in_descendants = False
                for user_node in user_nodes_:
                    if user_node in self.node_descendants:
                        user_nodes_in_descendants = True
                if user_nodes_in_descendants:
                    user_list_final[username] = u
            for username, rec in user_list_final.items():
                notif.append({'recipients': [rec['userId']],
                              "email": rec['email'],
                              "tenant_name": sec_context.name,
                              "nudge_name": "lock_unlock_status",
                              "lock_unlock_message": msg,
                              "skip_header_footer": True})
            send_notifications('lock_unlock_status', notif, epoch().as_xldate(),
                               tonkean=False)
        if records:
            return upload_forecast_schedule(nodes, records)

    def get_user_list(self):
        from utils.collab_connector_utils import get_dict_of_all_fm_users
        all_fm_users = get_dict_of_all_fm_users(consider_admin=False)
        return all_fm_users

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


class FMScheduleClass(FMSchedule):
    def __init__(self, period, nodes_having_changes,  *args, **kwargs):
        self.metrics = logger.new_metrics()
        self.debug = kwargs.get("debug", False)
        request = {'period': period,
                   'node': self.config.admin_node,
                   'nodes_having_changes': nodes_having_changes}
        self._get_metadata(request)
