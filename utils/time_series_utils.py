import logging

import numpy as np

from utils.misc_utils import index_of

logger = logging.getLogger("gnana.%s" % __name__)

def slice_timeseries(times, values, at, interpolate=False, use_fv=False, default=0):
    """
    get value at 'at' time in timeseries
    if 'at' not in timeseries, get value at closest prior time
    """
    if not times or not values:
        logger.warn("no timeseries data provided, is this a future period?")
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
