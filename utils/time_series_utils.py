import logging
from collections import namedtuple, defaultdict
import numpy as np

from utils.misc_utils import try_float, powerset, index_of

logger = logging.getLogger("gnana.%s" % __name__)
Event = namedtuple('Event', ['ts', 'event_type', 'data'])

def slice_timeseries(times, values, at, interpolate=False, use_fv=False, default=0):
    """
    get value at 'at' time in timeseries
    if 'at' not in timeseries, get value at closest prior time
    """
    if not times or not values:
        logger.warning("no timeseries data provided, is this a future period?")
        return default
    if at < times[0]:
        return default if not use_fv else values[0]
    if interpolate:
        try:
            return np.interp(at, times, values)
        except ValueError:
            idx = index_of(at, times)
            return values[idx]
    else:
        idx = index_of(at, times)
        return values[idx]


class EventAggregator:

    def __init__(self, **kwargs):
        self.events = []
        self.is_sorted = True
        self.until = kwargs.get('first_until', 0.0)

    def process_records(self, recs_list, final=True):
        self.events.extend([event for rec in recs_list for event in self.make_events(rec)])

        if final:
            self.events.sort()
            self.is_sorted = True
        else:
            self.is_sorted = False

    def make_events(self, rec):
        # Returns list of events. Event is a tuple where first thing is a ts.
        raise NotImplementedError

    def process_timeline(self):
        if not self.is_sorted:
            self.events.sort()
            self.is_sorted = True
        try:
            self.until = self.events[0].ts
        except:
            pass
        for event in self.events:
            self.process_event(event)

        self.try_finalize(force=True)

    def process_event(self, event):
        raise NotImplementedError

    def try_finalize(self, force=False):
        raise NotImplementedError


class LineItemEventAggregator(EventAggregator):
    """Simply event aggregator to calculate the value of line items based on groupbys."""

    THRESHOLD = 1.0 / 24

    def __init__(self, **kwargs):
        super(LineItemEventAggregator, self).__init__(**kwargs)

        self.ID = kwargs.get('ID')  # used for debug messages
        self.tot_amt_fld = kwargs.get('tot_amt_fld')
        self.li_amt_fld = kwargs.get('li_amt_fld')
        self.log_warnings = kwargs.get('log_warnings', True)

        self.grpby_flds = kwargs.get('grpby_fld', [])
        self.grpby_amt_fld = kwargs.get('grpby_amt_fld')
        self.li_in_flds = [self.li_amt_fld] + self.grpby_flds
        # TODO: Implement this.
        self.prune_zeros = kwargs.get('prune_zeros', False)

        # Raw Data
        self.recs = {}  # {split_record_id --> split_record}
        self.deleted_recs = {}  # {split_record_id --> deletion_time}
        self.processed_deletes = set()  # {... split_record_ids ... }

        # Internal State
        self.tot_amt = 0
        self.active_set = {} # {grp -> {record_id -> amt}}

        self.output_featmap = defaultdict(list)

    def make_events(self, rec):
        events = []
        # Creation. Data is record's values of all relevant fields & amt.
        self.recs[rec.ID] = rec
        events.append(Event(rec.created_date,
                            'cr',
                            rec.ID))
        # Deletion. Just goes into deleted recs map.
        # (Not an event since exact deletion time is unknown).
        if rec.getLatest('Stage') == 'SFDCDELETED':
            events.append(Event(rec.featMap['Stage'][-1][0],
                                'del',
                                rec.ID))
        for fld in self.li_in_flds:
            event_type = 'qty' if fld in [self.li_amt_fld] else 'fld'
            fld_hist = rec.featMap.get(fld, [])
            for i, (ts, val) in enumerate(fld_hist):
                # we only want to keep track of changes now as created handles the initial value
                if not i:
                    continue
                events.append(Event(ts,
                                    event_type,
                                    (rec.ID, fld, fld_hist[i - 1][1], val)))
        return events

    def process_event(self, event):
        if (event.ts - self.until) >= self.THRESHOLD:
            self.try_finalize()
        self.until = event.ts
        if event.event_type == 'cr':
            self.handle_creation(event)
        elif event.event_type == 'del':
            self.handle_deletion(event)
        elif event.event_type == 'qty':
            self.handle_qty(event)
        elif event.event_type == 'fld':
            self.handle_fld(event)
        else:
            raise Exception('Unknown event type.')

    def handle_creation(self, event):
        '''
        Adjust total amount.
        Add this record the active sets for each applicable grp.
        '''
        ID = event.data
        li_rec = self.recs[ID]
        # TODO: Do this in a way that allows for changing group vals.
        grp_fld_vals = [li_rec.getLatest(fld) for fld in self.grpby_flds]
        groups = ['_'.join(grp_fld_vals[:i+1])
                  for i, _v in enumerate(grp_fld_vals)]

        try:
            li_amt = try_float(li_rec.featMap[self.li_amt_fld][0][1])
        except KeyError:
            li_amt = 0.0

        self.tot_amt += li_amt

        for grp in groups:
            try:
                self.active_set[grp][event.data] = li_amt
            except:
                self.active_set[grp] = {event.data: li_amt}

    def handle_qty(self, event):
        """ Handles changes to amount."""
        # EVENT LOOKS LIKE (rec.ID, fld, prev_val, val)

        ID, fld, old_val, new_val = event.data
        if ID in self.processed_deletes:
            logger.warning('WARNING: Skipping handle qty event for deleted split record. %s', event)
            return
        old_val, new_val = try_float(old_val), try_float(new_val)

        li_rec = self.recs[ID]
        # TODO: Do this in a way that allows for changing group vals.
        grp_fld_vals = [li_rec.getLatest(fld) for fld in self.grpby_flds]
        groups = ['_'.join(grp_fld_vals[:i+1])
                  for i, _v in enumerate(grp_fld_vals)]

        delta = new_val - old_val
        for grp in groups:
            try:
                self.active_set[grp][ID] = new_val
            except:
                self.active_set[grp] = {ID: new_val}
        self.tot_amt += delta

    def handle_fld(self, event):
        """Handle changes in the groupby values. Not implemented."""
        logger.warning("Unable to handle groupby field change event with event data: %s.", event)
        pass

    def handle_deletion(self, event):
        '''
        Reduce total amount by current amount for the li.
        Remove li from all relevant groups. amount.
        '''
        _ts, _type, ID = event

        li_rec = self.recs[ID]
        # TODO: Do this in a way that allows for changing group vals.
        grp_fld_vals = [li_rec.getLatest(fld) for fld in self.grpby_flds]
        groups = ['_'.join(grp_fld_vals[:i+1])
                  for i, _v in enumerate(grp_fld_vals)]

        try:
            curr_amt = self.active_set[groups[0]][ID]
        except KeyError:
            curr_amt = 0.0

        self.tot_amt -= curr_amt

        for grp in groups:
            try:
                self.active_set[grp].pop(ID)
            except:
                raise Exception('Tried popping group=%s , ID=%s but active set was: %s' %
                                (grp, ID, self.active_set))

        self.processed_deletes.add(ID)

    def try_finalize(self, force=False):
        '''
        Every time the next event's timestamp is ahead, we write our current state to the out_flds.
        '''
        self.output_featmap[self.tot_amt_fld].append([self.until, self.tot_amt])
        for grp, grp_dtls in self.active_set.iteritems():
            try:
                grp_amt = sum(grp_dtls.values())
            except:
                grp_amt = 0.0

            self.output_featmap['%s_%s' % (self.grpby_amt_fld,
                                           grp)].append([self.until, grp_amt])


class SplitsEventAggregator(EventAggregator):

    THRESHOLD = 1.0 / 24

    def __init__(self, **kwargs):
        super(SplitsEventAggregator, self).__init__(**kwargs)

        self.ID = kwargs.get('ID')  # used for debug messages
        self.splt_tot_amt_fld = kwargs.get('splt_tot_amt_fld')
        self.splt_ratios_fld = kwargs.get('splt_ratios_fld')
        self.splt_amt_fld = kwargs.get('splt_amt_fld')
        self.splt_pct_fld = kwargs.get('splt_pct_fld')
        self.splt_flds = [(out_fld, dtls['in_fld']) for out_fld, dtls
                          in kwargs.get('dep_dds', {}).items()]
        self.splt_in_flds = [self.splt_pct_fld, self.splt_amt_fld] + [x[1] for x in self.splt_flds]
        self.log_warnings = kwargs.get('log_warnings', True)

        self.grpby_flds = kwargs.get('grpby_fld')
        self.type_fld = kwargs.get('type_fld', 'SplitTypeId')
        self.prune_zeros = kwargs.get('prune_zeros', False)
        self.all_grps = kwargs.get('all_grps', False)

        # Raw Data
        self.recs = {}  # {split_record_id --> split_record}
        self.deleted_recs = {}  # {split_record_id --> deletion_time}
        self.processed_deletes = set()  # {... split_record_ids ... }

        # Internal State
        self.tot_amt = 0
        self.active_set = {}  # {split_type_id --> {split_record_id --> status_of_split_dict}}
        # keep track of each type to validate it is adding up to 100%
        self.tot_splt_pct = {}  # {split_type_id --> sum_of_split_percentages_for_all_active_splits_for_split_type}
        self.output_featmap = defaultdict(list)

    def make_events(self, rec):
        events = []
        # Creation. Data is record's values of all relevant fields & amt.
        self.recs[rec.ID] = rec
        events.append(Event(rec.created_date,
                            'cr',
                            rec.ID))
        # Deletion. Just goes into deleted recs map.
        # (Not an event since exact deletion time is unknown).
        if 'Stage' in rec.featMap:
            stage_hist = rec.featMap['Stage']
            if stage_hist[-1][1] == 'SFDCDELETED':
                if self.splt_pct_fld:
                    self.deleted_recs[rec.ID] = stage_hist[-1][0]
                else:
                    events.append(Event(stage_hist[-1][0],
                                        'del',
                                        rec.ID))
        for fld in self.splt_in_flds:
            event_type = 'qty' if fld in [self.splt_amt_fld, self.splt_pct_fld] else 'fld'
            fld_hist = rec.featMap.get(fld, [])
            for i, (ts, val) in enumerate(fld_hist):
                # we only want to keep track of changes now as created handles the initial value
                if not i:
                    continue
                events.append(Event(ts,
                                    event_type,
                                    (rec.ID, fld, fld_hist[i - 1][1], val)))
        return events

    def process_event(self, event):
        if (event.ts - self.until) >= self.THRESHOLD:
            self.try_finalize()
        self.until = event.ts
        if event.event_type == 'cr':
            self.handle_creation(event)
        elif event.event_type == 'del':
            self.handle_deletion(event)
        elif event.event_type == 'qty':
            self.handle_qty(event)
        elif event.event_type == 'fld':
            self.handle_fld(event)
        else:
            raise Exception('Unknown event type.')

    def get_splt_types(self, splt_rec):
        '''
        Return splt type values for a split record. If there is a splt_type field,
        just return the value in that field. Otherwise return something like
        ['','_ValA','_ValA_ValB'] where ValA and ValB are the values in the two
        groupby fields.
        '''
        if self.grpby_flds:
            grp_vals = [splt_rec.getLatest(fld).replace(' ', '')
                        for fld in self.grpby_flds]
            if not self.all_grps:
                return [('_' if i else '') + '_'.join(grp_vals[:i])
                        for i in range(len(grp_vals) + 1)]
            else:
                return [('_' if vals else '') + '_'.join(vals)
                        for vals in powerset(grp_vals)]
        else:
            return [splt_rec.getLatest(self.type_fld)]

    def handle_creation(self, event):
        '''
        adjust total percentage in self.tot_splt_pct,
        insert ID into self.active_set
        update self.tot_amt
        '''
        splt_rec = self.recs[event.data]
        splt_types = self.get_splt_types(splt_rec)

        new_actv_dict = {out_fld: splt_rec.featMap.get(in_fld, [[splt_rec.created_date, 'N/A']])[-1][1]
                         for (out_fld, in_fld) in self.splt_flds}
        try:
	    splt_amt = try_float(splt_rec.featMap[self.splt_amt_fld][0][1])
        except (KeyError, IndexError):
	    logger.warning('No amount field found for Split Record %s', event.data)
	    splt_amt = 0.0
        new_actv_dict['_amt'] = splt_amt
        if self.splt_pct_fld:
            splt_pct = try_float(splt_rec.featMap[self.splt_pct_fld][0][1])
            new_actv_dict['_pct'] = splt_pct

        self.tot_amt += splt_amt

        for splt_type in splt_types:
            try:
                self.active_set[splt_type][event.data] = new_actv_dict
                if self.splt_pct_fld:
                    self.tot_splt_pct[splt_type] += splt_pct
            except KeyError:
                self.active_set[splt_type] = {event.data: new_actv_dict}
                if self.splt_pct_fld:
                    self.tot_splt_pct[splt_type] = splt_pct

    def handle_qty(self, event):
        """ Handles changes to either quantity field: either splt_amt_fld or
        splt_pct_fld."""
        # EVENT LOOKS LIKE (rec.ID, fld, fld_hist[i-1][1], val)

        ID, fld, old_val, new_val = event.data
        if ID in self.processed_deletes:
            logger.warning('WARNING: Skipping handle qty event for deleted split record. %s', event)
            return
        old_val, new_val = try_float(old_val), try_float(new_val)

        splt_rec = self.recs[ID]
        splt_types = self.get_splt_types(splt_rec)

        qty = '_amt' if fld == self.splt_amt_fld else '_pct'

        for splt_type in splt_types:
            self.active_set[splt_type][ID][qty] = try_float(new_val)
            if qty != '_amt':
                self.tot_splt_pct[splt_type] += new_val - old_val
        if qty == '_amt':
            self.tot_amt += new_val - old_val

    def handle_fld(self, event):
        # EVENT LOOKS LIKE (rec.ID, fld, fld_hist[i-1][1], val)

        ID, fld, _old_val, new_val = event.data
        if ID in self.processed_deletes:
            logger.warning('WARNING: Skipping handle fld event for deleted split record. %s', event)
            return

        splt_rec = self.recs[ID]
        splt_types = self.get_splt_types(splt_rec)

        for splt_type in splt_types:
            self.active_set[splt_type][ID][fld] = new_val

    def handle_deletion(self, event):
        '''
        reduces total percentage in self.tot_splt_pct,
        removes ID from self.active_set
        decreases self.tot_amt
        '''
        _ts, _type, ID = event

        splt_rec = self.recs[event.data]
        splt_types = self.get_splt_types(splt_rec)

        splt_amt = 0.0
        for splt_type in splt_types:
            try:
                splt_amt = self.active_set[splt_type][ID]['_amt']
                if self.splt_pct_fld:
                    splt_pct = self.active_set[splt_type][ID]['_pct']
            except:
                raise Exception('Tried popping type=%s , ID=%s but active set was: %s' %
                                (splt_type, ID, self.active_set))

            self.active_set[splt_type].pop(ID)
            if self.splt_pct_fld:
                self.tot_splt_pct[splt_type] -= splt_pct
        self.tot_amt -= splt_amt
        self.processed_deletes.add(ID)

    def guess_deleted(self, active_recs):
        '''
        This can be made arbitrarily complicated as needed but for now we
        basically get rid of the first one sorted alphabetically
        '''
        return sorted(active_recs)[0]

    def try_finalize(self, force=False):
        '''
        every time there is progression in timestamp, we either
        1) write a new state to output fields (if the current state is good)
        or 2) incrementally improve the state and make a recursive call.

        A state is 'good' if every split type has a total percentage under 100.
        If it is less than 100, we warn and scale up.

        Termination is guaranteed because every call 'improves' the state by removing
        at least one split from the active set, guessing if necessary. Eventually it
        here will be less than 100% for each type.

        for this sub-class force parameter is ignored

        '''

        for splt_type, tot_pct in self.tot_splt_pct.items():
            excess = tot_pct - 100.0
            if excess < 0:
                if self.log_warnings:
                    logger.warning('OpportunityId %s in a bad state. Less than 100 pct splits for type %s at time %s.' +
                                   'Splits will be scaled up.',
                                   self.ID, splt_type, self.until)
            elif excess > 0:
                actv_splts = set(self.active_set[splt_type])
                remain_to_be_deleted_splts = [(ts, ID) for ID, ts in self.deleted_recs.items()
                                              if ID in actv_splts]
#                                                  self.active_set[splt_type][ID]['_pct'] <= excess)]
                if remain_to_be_deleted_splts:
                    remain_to_be_deleted_splts.sort()

                    # should we ensure that the date is AFTER the current
                    # self.until so we know we haven't screwed up already?

                    self.handle_deletion(Event(self.until, 'del', remain_to_be_deleted_splts[0][1]))
                    self.try_finalize(self)
                    return
                elif force:
                    logger.warning('OpportunityId %s ended in an inconsistent state for splt_type %s. (Events: %s)',
                                   self.ID,
                                   splt_type,
                                   self.events)
#                     id_to_delete = self.guess_deleted(self.active_set[splt_type])
#                     if self.log_warnings:
#                         logger.warning('Had to guess a deletion for ID=%s at time %s. Guessed %s from options: %s',
#                                        self.ID, self.until, id_to_delete, self.active_set[splt_type].keys())
#                     self.handle_deletion(Event(self.until, 'del', id_to_delete))
#                     self.try_finalize(self)
#                     return

        # Everything is copacetic
        self.output_featmap[self.splt_tot_amt_fld].append([self.until, self.tot_amt])
        for (out_fld, _) in self.splt_flds:
            # trans_map : {splt_record_id --> value of output field (e.g. owner_id)}
            trans_map = {}
            for splt_type, recs_dict in self.active_set.items():
                trans_map.update({k: v[out_fld] for k, v in recs_dict.items()})
            self.output_featmap[out_fld].append([self.until, trans_map])

        if self.grpby_flds:
            for splt_type, recs_dict in self.active_set.items():
                pct_map = {k: (v['_amt'] / self.tot_amt) if self.tot_amt else (v.get('_pct', 100) / 100. / len(self.active_set))
                           for k, v in recs_dict.items()}
                if self.prune_zeros:
                    pct_map = {k: v for k, v in pct_map.items() if v != 0.0}
                self.output_featmap[self.splt_ratios_fld + splt_type].append([self.until, pct_map])
        else:
            pct_map = {}
            for splt_type, recs_dict in self.active_set.items():
                pct_map.update({k: (v['_amt'] / self.tot_amt) if self.tot_amt else (v.get('_pct', 100) / 100. / len(self.active_set))
                                for k, v in recs_dict.items()})
                if self.prune_zeros:
                    pct_map = {k: v for k, v in pct_map.items() if v != 0.0}
            self.output_featmap[self.splt_ratios_fld].append([self.until, pct_map])
