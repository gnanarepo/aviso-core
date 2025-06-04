from domainmodel import Model
from utils.date_utils import EpochClass
import logging
from datetime import timedelta

logger = logging.getLogger('gnana.%s' % __name__)


class StringMaps(Model):
    tenant_aware = True
    collection_name = 'stringmap'
    version = 1
    kind = 'domianmodel.stringmap.StringMaps'
    encrypted = False

    def __init__(self, attrs=None):
        self.field_name = ""
        self.maps = {}
        self.rev_maps = {}
        super(StringMaps, self).__init__(attrs)

    def encode(self, attrs):
        attrs['field_name'] = self.field_name
        attrs['strings'] = self.maps.items()
        super(StringMaps, self).encode(attrs)

    def decode(self, attrs):
        self.maps = dict(attrs['strings'])
        self.field_name = attrs['field_name']
        self.rev_maps = [(v, k) for k, v in self.maps.items()]
        return super(StringMaps, self).decode(attrs)

    @classmethod
    def add_map(cls, field, value):
        x = StringMaps.getByFieldValue('field_name', field)

        if not x:
            x = StringMaps()
            x.field_name = field
            x.maps = {}
        if value in x.maps:
            return x.maps[value]
        string_id = len(x.maps)
        x.maps = dict(list(x.maps.items()) + [(value, string_id)])
        x.save()
        return string_id


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


# need to update list manually if new parser included
def get_all_parser():
    all_parsers = []
    for parser,func in parse_fns.items():
        all_parsers.append(parser)

    return all_parsers

def _parse(val, fn, parser_fallback_lambda=None):
    if fn not in parse_fns:
        # Trying to see if the input parser is a lambda fn or not.
        try:
            return eval(fn)(val)
        except Exception as e:
            logger.warning(
                'Not a valid Parser function, tried as lambda function but failed. Exception %s' % (e))
        raise Exception('The Parser function ' + fn + ' is not defined in the maps')
    else:
        try:
            return parse_fns[fn](val)
        except Exception as e:
            if parser_fallback_lambda:
                return eval(parser_fallback_lambda)(val)
            raise e


def parse_record_fields(ds, record):
    """
    Converts the fields in the record into their respective data types.
    """
    for key in record:
        if key in ds.fields and "type" in ds.fields[key]:
            record[key] = _parse(record.get(key), ds.fields[key]['type'])
    return record
