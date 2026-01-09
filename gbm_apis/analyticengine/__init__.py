from collections import Counter, defaultdict
import logging
from math import exp
from gbm_apis.deal_result.splitter_service import ViewGeneratorService
from gbm_apis.deal_result.viewgen_service import CoolerViewGeneratorService

basestring = str
long = int


def unicode_conversion(text):
    if isinstance(text, bytes):
        return str(text, 'utf-8')
    else:
        return text

unicode = unicode_conversion

config_restrict_paths = ['datasets.<>.maps', 'datasets.<>.filetypes']


logger = logging.getLogger('gnana.%s' % __name__)


class AnalysisModel(object):

    """ Base class for all analysis models

    All implementation class must define some metadata
    allow_aggregation: Dimensions will be honored by this model
    predicts: Produces future values.  Horizon is important.
    compares_with_history: Compares each record with history
    produce_individual_result: If True, stores individual records;
                               If False, stores group results.

    .. graphviz::

        digraph "Known Models" {
            rankdir = BT
            node [shape=record]
            ForecastBaseModel -> AnalysisModel
            ForecastModel -> ForecastBaseModel
            Forecast2Model -> ForecastBaseModel
            UnbornModel -> ForecastBaseModel
            RecordCounter -> AnalysisModel
        }

    """
    combine_options = []
    ignore_closure_errors = False
    def __init__(self, field_map, config, time_horizon, dimensions):

        self.cache = {}
        self.probe = False
        self.dimensions = dimensions or []
        self.time_horizon = time_horizon
        self.config = config
        self.require_post_process = False
        self.splitter_config = config.get('splitter_config', {}).copy()
        self.splitter_config["uip_only"] = True
        self.drilldown_config = config.get('ds_params', {}).get('general', {}).get("drilldown_config", {})
        self.drilldown_config["drilldowns"] = self.dimensions

        hs_impl = self.drilldown_config.get('hs_impl', 'G')
        if hs_impl == 'A':
            self.viewgen_svc = CoolerViewGeneratorService(hier_asof=self.time_horizon.as_ofF)
        else:
            self.viewgen_svc = ViewGeneratorService(
                drilldown_config=self.drilldown_config,
                splitter_config=self.splitter_config,
                base_ts=self.time_horizon.as_ofF,
            )
            self.drilldown_flds = self.viewgen_svc.hier_svc.component_fields
            self.level_groupby_flds = self.viewgen_svc.hier_svc.level_groupby_flds


    def cached_or_not(self, extid, observation_time, name, fn):
        try:
            return self.cache[(extid, observation_time, name,)]
        except KeyError:
            val = fn()
            self.cache[(extid, observation_time, name,)] = val
            return val

    # Following methods are created to assist in development of new
    # models with the right signature
    def is_active(self, record):
        """ Called by the framework to decide if the record is considered
        active by the model
        """
        raise NotImplementedError()

    def is_history(self, record):
        """ Called by the framework to decide if the record is considered
        historical by the model
        """
        raise NotImplementedError()

    def is_closed(self, record):
        """ Called by the framework to decide if the record is closed
        in the current period
        """
        raise NotImplementedError()

    def is_future(self, record):
        """ Called by the framework to decide if the record is in the future
        and should simply be ignored
        """
        return False

    def process_observation(self, active_record, obs):
        """ Called by the framework to let the model process the observation
        visa-vis the active record.

        For models that compare with history, both active_record and
        observation will be present.

        For models that do not compare with history, either the active_record,
        or the observation is present.
        """
        raise NotImplementedError()

    def process_closure(self, active_record):
        """ Called to process a record closed in the current period """
        raise NotImplementedError()

    def create_observations(self, record):
        """
        Observations are passed back for the model to process, and they are
        pass through objects for the caller.  We are simply returning number
        of timepoints we are interested in. When we get the observation, we
        need to know which record, hence record is included in the returned
        comprehension.

        If no observations return blank.  For performance reasons, None
        is not checked by the caller

        Note that, framework expects the observation to be iterable.

        :param record: Record for which observations are to be created
        """
        raise NotImplementedError()

    def get_total_results(self):
        """ Called to get the total of all the records processed.
        """
        raise NotImplementedError()

    def combine_results(self, results, combine_options):
        """ Called to combine the results from multiple workers into a
        single result
        """
        raise NotImplementedError()

    def get_result(self, record):
        """ Called to retrieve the results for a single record.

        This is called only when the model declares comparison with history.
        For models that do not compare, unit results are not applicable
        """
        raise NotImplementedError()

    def prepare(self, record):
        """
        Gives an opportunity for the model prepare the record.  It will
        be cheaper to pre-create any data needed during prepare.
        """
        pass

    def determine_deps(self):
        """
        Gives the model an opportunity to alert the task v2 framework that
        it will need the results of additional dependent tasks.
        """
        return {}

    def incorporate_deps(self, dep_results):
        """
        Gives the model an opportunity to incorporate any information needed from
        the results of dependent tasks into its state.
        """
        return


class DimensionalCounter(object):

    """ Automatically maintains a list of counters, and assigns items into,
    total and the selected dimensional values.
    """

    def __init__(self, dimensions, th=None, debug=False):
        self.counters = defaultdict(Counter)
        self.dimensions = {}
        self.dim_flds = set()
        if dimensions and hasattr(dimensions, '__iter__'):
            for dim in dimensions:
                if dim and isinstance(dim, basestring):
                    dim_flds = dim.split('~')
                    self.dim_flds |= set(dim_flds)
                    for idx in range(len(dim_flds)):
                        dim_key = '~'.join(["%s" % x for x in dim_flds[:idx + 1]])
                        self.dimensions[dim_key] = dim_flds[:idx + 1]
        self.th = th
        self.debug = debug

    def __iter__(self):
        return self.counters.__iter__()

    def __getitem__(self, key):
        return self.counters[key]

    def _get_fld_val(self, record, fld):
        if fld.startswith('as_of_fv_'):
            return record.getAsOfDateF(fld[9:], self.th.as_ofF, first_value_on_or_after=True)
        elif fld.startswith('as_of_'):
            return record.getAsOfDateF(fld[6:], self.th.as_ofF, first_value_on_or_after=False)
        elif fld.startswith('begins_fv_'):
            return record.getAsOfDateF(fld[9:], self.th.beginsF, first_value_on_or_after=True)
        elif fld.startswith('begins_'):
            return record.getAsOfDateF(fld[6:], self.th.beginsF, first_value_on_or_after=False)
        elif fld.startswith('horizon_fv_'):
            return record.getAsOfDateF(fld[11:], self.th.horizonF, first_value_on_or_after=True)
        elif fld.startswith('horizon_'):
            return record.getAsOfDateF(fld[8:], self.th.horizonF, first_value_on_or_after=False)
        elif fld.startswith('latest_'):
            return record.getLatest(fld[7:])
        elif fld.startswith('frozen_'):
            return record.getAsOfDateF(fld[7:], self.th.as_ofF, first_value_on_or_after=True)
        else:  # assume we want to use latest
            return record.getLatest(fld)

    def increment(self, value, record, as_ofF, amount=1, counting=False):
        try:
            self.counters["."][value] += amount
        except:
            if self.counters["."][value] in [0, 0.0]:
                self.counters["."][value] = amount
            else:
                raise RuntimeError('two values are not summable')
        if self.debug:
            try:
                self.counters[value + "_contribs"][record.ID + str(int(as_ofF))] += amount
            except:
                if self.counters[value + "_contribs"][record.ID + str(int(as_ofF))] in [0, 0.0]:
                    self.counters[value + "_contribs"][record.ID + str(int(as_ofF))] = amount
                else:
                    raise RuntimeError('two values are not summable')
        rec = {x: self._get_fld_val(record, x) for x in self.dim_flds}
        if self.dimensions:
            for dim_str, dim_flds in self.dimensions.items():
                key = dim_str + "." + '~'.join([str(rec[x]) for x in dim_flds])
                try:
                    self.counters[key][value] += amount
                except:
                    if self.counters[key][value] in [0, 0.0]:
                        self.counters[key][value] = amount
                    else:
                        raise RuntimeError('two values are not summable')

    def get_counts(self):
        return self.counters



def get_decay_factors(as_of, as_of_times, horizon_times, decay, seasonality):

    if len(as_of_times) != len(horizon_times):
        raise RuntimeError('the length of the as_of_times and horizon_times are different')
    num_dates = len(as_of_times)
    seasonality_mult = list(range(num_dates))
    decay_factor = list(range(num_dates))
    for i in range(num_dates):
        seasonality_mult[-i - 1] = seasonality[(-i - 1) % len(seasonality)]
    num_seasons = float(len(seasonality))

    for k in range(num_dates):
        t1 = as_of_times[k]
        t2 = horizon_times[k]
        decay_factor[k] = decay / num_seasons * (t1 - as_of)
        decay_factor[k] = seasonality_mult[k] * exp(decay_factor[k])

    return decay_factor