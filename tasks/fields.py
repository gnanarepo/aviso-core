from domainmodel import Model
from utils.date_utils import EpochClass
import logging
from datetime import timedelta

logger = logging.getLogger('gnana.%s' % __name__)


def date_parser(value, out_type='epoch', fmt='%Y%m%d', add_seconds=None):
    ep = EpochClass.from_string(value, fmt, timezone='tenant')
    if add_seconds and isinstance(add_seconds, int):
        ep += timedelta(seconds=add_seconds)
    return ep.as_xldate() if out_type == 'xl' else ep.as_epoch()


def yyyymmdd_to_epoch(value):
    return date_parser(value, 'epoch')


def yyyymmdd_to_xl(value):
    return date_parser(value, 'xl', add_seconds=1)


def xl_to_epoch(value):
    ep = EpochClass.from_xldate(float(value))
    return ep.as_epoch()


def epoch_to_xl(value):
    ep = EpochClass.from_epoch(float(value))
    return ep.as_xldate()


def currency(value, cur_sym='$'):
    return round(float(str(value).replace(',', '').replace(cur_sym, '')), 2)


def parse_field(val, field_config):
    if field_config and field_config.parser:
        return _parse(val, field_config.parser, field_config.parser_fallback_lambda)
    else:
        return val


parse_fns = {
    'int': lambda x: int(float(x)),
    'long': lambda x: int(x),
    'float': lambda x: float(x),
    'yyyymmdd_to_xl': yyyymmdd_to_xl,
    'yyyymmdd_to_epoch': yyyymmdd_to_epoch,
    'epoch_to_xl': epoch_to_xl,
    'xl_to_epoch': xl_to_epoch,
    'str': str,
    'string': str,
    'ccy': currency,
}


def _parse(val, fn, parser_fallback_lambda=None):
    if fn not in parse_fns:
        # Trying to see if the input parser is a lambda fn or not.
        try:
            return eval(fn)(val)
        except Exception as e:
            logger.warning(f'Not a valid Parser function, tried as lambda function but failed. Exception {e}')
        raise Exception('The Parser function ' + fn + ' is not defined in the maps')
    else:
        try:
            return parse_fns[fn](val)
        except Exception as e:
            if parser_fallback_lambda:
                return eval(parser_fallback_lambda)(val)
            raise e
