from logging import Formatter, getLogger, Logger
import logging.config
import os
import threading
import uuid
import inspect
import sys
import numbers
import time


class Metriclogger(Logger):
    """
    Drop in replacement for the logger class for Aviso 'AvisoFullLogFormatter' based logs
    This extends the logger class to add structured metric information to the logs for easier parsing

    DEBUG/INFO/WARN/ERROR all now have been extended to take additional metric information. In addition
    new METRIC log level is added.

    Timers/Counters/Metric are all logged at the new METRIC level.

    Example Usage:
    >>>
    >>> logging.setLoggerClass(Metriclogger)
    >>> logger = logging.getLogger('myclass')

    >>>with logger.timed("pass_timed"):
    >>>     # add the code you wish to time here
    >>>    pass
    >>>
    >>> with logger.timed("pass_timed_with_optional_tags", tags={"tag1":"value1", "tag2":"value2"}):
    >>>    pass
    >>>
    >>> logger.timer("just.some.timer", 10)
    >>>
    >>> metrictimer = logger.new_timer()
    >>> # stuff to time here.
    >>> metrictimer.stop()  # stopping not required, logger will also read the elapsed value of a non stopped timer
    >>> # do other stuff and finally log it
    >>> logger.timer("just.some.metrictimer", metrictimer)
    >>> logger.timer("just.some.metrictimer", metrictimer, msg="optional message", tags={"more_optional": "tag_values"})
    >>>
    >>> logger.counter("mycounter", 0.5, msg="counters message", tags={"more_optional": "tag_values"})
    >>>
    >>> # new optional parameters counters, timers, tags
    >>> logger.debug('debug message', counters={"c": 3, "d": 5.00}, timers={"tc": 3, "td": 4}, tags={"key1": "val1", "key2": "val2"})
    >>> logger.info('info message', counters={"c": 3, "d": 5.00}, timers={"tc": 3, "td": 4}, tags={"key1": "val1", "key2": "val2"})
    >>> logger.warning('warning message', counters={"c": 3, "d": 5.00}, timers={"tc": 3, "td": 4}, tags={"key1": "val1", "key2": "val2"})
    >>> logger.error('error message', counters={"c": 3, "d": 5.00}, timers={"tc": 3, "td": 4}, tags={"key1": "val1", "key2": "val2"})
    >>>
    """
    def __init__(self, name):
        super(Metriclogger, self).__init__(name)
        logging.METRIC = 35  # above warning (30) and below Error (40)
        logging.addLevelName(logging.METRIC, 'METRIC')
        pass

    def debug(self, msg, *args, **kwargs):
        """
        Due to a python2 limitation (supported in 3) the function def should be considered as:
        def debug(self, msg, *args, counters=None, timers=None, tags=None,  **kwargs):
         or
        def debug(self, msg, *args, metrics=None,  **kwargs):
        """
        metrics_set = kwargs.pop('metrics', None)
        if metrics_set:
            counters, timers, tags = metrics_set.format()
        else:
            counters, timers, tags = kwargs.pop('counters', None), kwargs.pop('timers', None), kwargs.pop('tags', None)

        Logger.debug(self, self.__format_msg(msg, counters, timers, tags), *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        """
        Due to a python2 limitation (supported in 3) the function def should be considered as:
        def info(self, msg, *args, counters=None, timers=None, tags=None,  **kwargs):
         or
        def info(self, msg, *args, metrics=None,  **kwargs):
        """
        metrics_set = kwargs.pop('metrics', None)
        if metrics_set:
            counters, timers, tags = metrics_set.format()
        else:
            counters, timers, tags = kwargs.pop('counters', None), kwargs.pop('timers', None), kwargs.pop('tags', None)
            
        Logger.info(self, self.__format_msg(msg, counters, timers, tags), *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        """
        Due to a python2 limitation (supported in 3) the function def should be considered as:
        def warning(self, msg, *args, counters=None, timers=None, tags=None,  **kwargs):
         or
        def warning(self, msg, *args, metrics=None,  **kwargs):
        """
        metrics_set = kwargs.pop('metrics', None)
        if metrics_set:
            counters, timers, tags = metrics_set.format()
        else:
            counters, timers, tags = kwargs.pop('counters', None), kwargs.pop('timers', None), kwargs.pop('tags', None)

        Logger.warning(self, self.__format_msg(msg, counters, timers, tags), *args, **kwargs)

    def warn(self, msg, *args, **kwargs):
        """
        Due to a python2 limitation (supported in 3) the function def should be considered as:
        def warn(self, msg, *args, counters=None, timers=None, tags=None,  **kwargs):
         or
        def warn(self, msg, *args, metrics=None,  **kwargs):
        """
        metrics_set = kwargs.pop('metrics', None)
        if metrics_set:
            counters, timers, tags = metrics_set.format()
        else:
            counters, timers, tags = kwargs.pop('counters', None), kwargs.pop('timers', None), kwargs.pop('tags', None)

        Logger.warning(self, self.__format_msg(msg, counters, timers, tags), *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        """
        Due to a python2 limitation (supported in 3) the function def should be considered as:
        def error(self, msg, *args, counters=None, timers=None, tags=None,  **kwargs):
         or
        def error(self, msg, *args, metrics=None,  **kwargs):
        """
        metrics_set = kwargs.pop('metrics', None)
        if metrics_set:
            counters, timers, tags = metrics_set.format()
        else:
            counters, timers, tags = kwargs.pop('counters', None), kwargs.pop('timers', None), kwargs.pop('tags', None)

        Logger.error(self, self.__format_msg(msg, counters, timers, tags), *args, **kwargs)

    def new_metrics(self, prefix=None):
        """
        :param prefix: an optional prefix that will be prepended to all metric names
        returns an instance of MetricSet with counters, timers, tags attributes to be use as a set with the metrics method.
        """
        return MetricSet(prefix)

    def metrics(self, msg, *args, **kwargs):
        """
        Due to a python2 limitation (supported in 3) the function def should be considered as:
        def metrics(self, msg, *args, counters=None, timers=None, tags=None,  **kwargs):

        Records a log entry under the METRIC level with the supplied metrics
        """

        metrics_set = kwargs.pop('metrics', None)
        if metrics_set:
            counters, timers, tags = metrics_set.format()
        else:
            counters, timers, tags = kwargs.pop('counters', None), kwargs.pop('timers', None), kwargs.pop('tags', None)

        Logger.log(self, logging.METRIC, self.__format_msg(msg, counters, timers, tags), *args, **kwargs)

    def counter(self, name, value, msg="", tags=None):
        """
       Records a counter metric to the log at the METRIC level
       :param name: the name of the metric
       :param value: the value of the counter
       :param msg: Optional message for log
       :param tags: Optional tags of type dict for log
       """

        if not isinstance(value, numbers.Number):
            raise ValueError("counter must be a number")

        self.metrics(self.__format_msg(msg, {name: value}, None, tags))

    def new_timer(self):
        """
        :return: a new timer to be used with the .timer() method
        """
        return MetricTimer()

    def timer(self, name, elapsed, msg="", tags=None):
        """
        Records a timer metric to the log at the METRIC level
        :param name: the name of the timer
        :param elapsed: the time elapsed in MS or a timer created with new_timer()
        :param msg: Optional message for log
        :param tags: Optional tags of type dict for log
        """

        if not isinstance(elapsed, numbers.Number) and not isinstance(elapsed, MetricTimer):
            raise ValueError("timer must be a number or MetricTimer")

        if not tags:
            self.metrics("{0}||{1}={2:.3f}".format(msg, name, float(elapsed)))
        else:
            self.metrics("{0}||{1}={2:.3f}|{3}".format(msg, name, float(elapsed), self.__format_dict(tags)))

    def timed(self, name, msg="", tags=None):
        """
        Measure the time taken in a block of code
            with logger.timed('latency'):
                measure_me_func()

        This records a timer metric to the log at the METRIC level
        :param name: the name of the timer
        :param msg: Optional message for log
        :param tags: Optional tags of type dict for log
        """

        logger_self = self

        class timer_context:

            def __enter__(self):
                self.start_time = time.time()

            def __exit__(self, exc_type, exc_value, tb):
                elapsed = (time.time() - self.start_time) * 1000
                logger_self.timer(name, elapsed, msg=msg, tags=tags)

        return timer_context()


    def findCaller(self, frame):
        """
        Find the stack frame of the caller so that we can note the source
        file name, line number and function name.
        """
        f = currentframe()
        #On some versions of IronPython, currentframe() returns None if
        #IronPython isn't run with -X:Frames.
        if f is not None:
            f = f.f_back
        rv = "(unknown file)", 0, "(unknown function)", None
        while hasattr(f, "f_code"):
            co = f.f_code
            filename = os.path.normcase(co.co_filename)
            if filename == _srcfile or filename == _logging_srcfile:
                f = f.f_back
                continue
            rv = (co.co_filename, f.f_lineno, co.co_name, None)
            break
        return rv

    def __format_msg(self, msg, counters, timers, tags):
        if tags:
            return "{0}|{1}|{2}|{3}".format(msg, self.__format_counters(counters), self.__format_timers(timers), self.__format_dict(tags))
        if timers:
            return "{0}|{1}|{2}".format(msg, self.__format_counters(counters), self.__format_timers(timers))
        if counters:
            return "{0}|{1}".format(msg, self.__format_counters(counters))
        return msg

    def __format_dict(self, dictionary):
        if dictionary:
            return ','.join("%s=%s" % (k, v) for k, v in dictionary.items())
        return ""

    def __format_counters(self, numbers_dict):
        if numbers_dict:
            return ','.join("{0}={1}".format(k, str(v)) for k, v in numbers_dict.items())
        return ""

    def __format_timers(self, timer_dict):
        if timer_dict:
            return ','.join("{0}={1:.3f}".format(k, float(v)) for k, v in timer_dict.items())
        return ""

    def __is_this_logger__(self, outer):
        is_logger = 'self' in outer[0].f_locals and outer[0].f_locals['self'] is self
        is_timer = 'logger_self' in outer[0].f_locals and outer[0].f_locals['logger_self'] is self
        return is_logger or is_timer


class MetricSet:
    def __init__(self, prefix=None):
        self.prefix = prefix
        self.counters = {}
        self.timers = {}
        self.tags = {}

    def with_prefix(self, prefix):
        """
        Create a child metric set that is a prefixed instance of this one
        """

        if self.prefix is not None:
            child_set = MetricSet(self.prefix + "." + prefix)
        else:
            child_set = MetricSet(prefix)

        # force shallow copy
        child_set.counters = self.counters
        child_set.timers = self.timers
        child_set.tags = self.tags
        return child_set

    def format(self):
        return self.counters, self.timers, self.tags

    def inc_counter(self, counter, inc=1):
        if self._key(counter) not in self.counters:
            self.counters[self._key(counter)] = inc
        else:
            self.counters[self._key(counter)] = self.counters[self._key(counter)] + inc

    def set_counter(self, counter, val):
        self.counters[self._key(counter)] = val

    def new_timer(self, timer_name):
        timer = MetricTimer()
        self.timers[self._key(timer_name)] = timer
        return timer

    def set_timer(self, timer_name, timer):
        self.timers[self._key(timer_name)] = timer

    def _key(self, keyname):
        if self.prefix is None:
            return keyname
        return self.prefix + '.' + keyname


class NOOPMetricSet(MetricSet):
    def __init__(self, prefix=None):
        pass

    def with_prefix(self, prefix):
        return self

    def format(self):
        return {}, {}, {}

    def inc_counter(self, counter, inc=1):
        pass

    def set_counter(self, counter, val):
        pass

    def new_timer(self, timer_name):
        return NOOPMetricTimer()

    def set_timer(self, timer_name, timer):
        pass


class MetricTimer:
    """
    A simple timer class to be used with the Metriclogger
    """
    def __init__(self):
        self.start_time = time.time()
        self.end_time = 0

    def reset(self):
        self.start_time = time.time()
        self.end_time = 0
        return self

    def stop(self):
        self.end_time = time.time()
        return self

    def elapsed(self):
        if self.end_time != 0:
            return (self.end_time - self.start_time) * 1000
        return (time.time() - self.start_time) * 1000

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_value, tb):
        self.stop()

    def __str__(self):
        return "{0:.3f}".format(self.elapsed())

    def __float__(self):
        return self.elapsed()


class NOOPMetricTimer(MetricTimer):
    """
      A NO-OP timer class to be used with the Metriclogger
      """
    def __init__(self):
        pass # intentionally do not call base __init__

    def reset(self):
        return self

    def stop(self):
        return self

    def elapsed(self):
        return 0

    def __exit__(self, exc_type, exc_value, tb):
        pass


if hasattr(sys, 'frozen'):  # support for py2exe
    _srcfile = "logging%s__init__%s" % (os.sep, __file__[-4:])
elif __file__[-4:].lower() in ['.pyc', '.pyo']:
    _srcfile = __file__[:-4] + '.py'
else:
    _srcfile = __file__
_srcfile = os.path.normcase(_srcfile)

_logging_srcfile = getattr(logging, "_srcfile", None)

def currentframe():
    """Return the frame object for the caller's stack frame."""
    try:
        raise Exception
    except:
        return sys.exc_info()[2].tb_frame.f_back
