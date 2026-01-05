__author__ = 'mael'

from ..analyticengine.CombineResults import CombineResults
from ..analyticengine.forecast2 import Forecast2


class Forecast5(Forecast2):
    """This is used for all GBM tenants."""

    def __init__(self, field_map, config, time_horizon, dimensions):
        super(Forecast5, self).__init__(field_map, config, time_horizon, dimensions)

    def get_total_results(self):
        pass

    def combine_results(self, results, combine_options):
        return CombineResults.combine_results(results, combine_options)
