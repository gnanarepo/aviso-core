from django.http import HttpResponseBadRequest

from config.fm_config import FMConfig
from config.deal_config import DealConfig
from infra.read import get_current_period, node_is_valid
from infra.read import period_is_active as active_period
from infra.read import validate_period
from utils.common import MicroAppView, cached_property


class FMView(MicroAppView):
    """
    Base class for all fm service views
    """
    validators = []

    @cached_property
    def config(self):
        return FMConfig(debug=self.debug)

    @cached_property
    def deal_config(self):
        return DealConfig()


# Error Codes
UPLOAD_ERROR_400 = "Tried to upload for a non-user-entered field."
INVALID_PERIOD = 'The provided period is not valid'
INVALID_NODE = 'No such node for the specified quarter'
INACTIVE_PERIOD = 'The provided period is not an active period'
MALFORMED_RECORDS = 'The uploaded records are not in the correct format, fields required: {}'
EXCEL_FORMAT_CHANGE = 'Please ensure the date column headers must be in yyyy-mm-dd format'


# API Validators
def field_is_user_entered(request, config):
    field = request.GET.get('field')
    if field not in config.user_entered_fields and field not in config.forecast_service_editable_fields:
        return HttpResponseBadRequest(UPLOAD_ERROR_400)


def period_is_active(request, config):
    period = request.GET.get('period', get_current_period())
    if not active_period(period, config=None, quarter_editable=config.quarter_editable,
                         component_periods_editable=config.component_periods_editable,
                         future_quarter_editable=config.future_qtrs_editable_count,
                         past_quarter_editable=config.past_qtrs_editable_count):
        return HttpResponseBadRequest(INACTIVE_PERIOD)


def period_is_valid(request, config):
    period = request.GET.get('period', get_current_period())
    if not validate_period(period):
        return HttpResponseBadRequest(INVALID_PERIOD)


def node_is_editable(request, config):
    node = request.GET.get('node')
    period = request.GET.get('period', get_current_period())
    if not node_is_valid(node, period):
        return HttpResponseBadRequest(INVALID_NODE)


def node_can_access_field(request, config):
    node = request.GET.get('node')
    fields = request.GET.getlist('field')
    # TODO: me


def records_are_valid(records, config):
    required_fields = ['period', 'node', 'field', 'val']
    for record in records:
        if any((field not in record for field in required_fields)):
            return HttpResponseBadRequest(MALFORMED_RECORDS.format(required_fields))
        if record['field'] not in config.user_entered_fields and record['field'] not in config.forecast_service_editable_fields:
            return HttpResponseBadRequest(UPLOAD_ERROR_400)


def validating_excel_column_headers(field):
    default_fields = {'period', 'node', 'label'}
    if field not in default_fields:
        if len(field) != 10:
            return EXCEL_FORMAT_CHANGE
        if (field[4] != '-' or field[7] != '-'):
            return EXCEL_FORMAT_CHANGE

def get_all_waterfall_fields():
    default_fields = ['type']
    for i in range(1, 14):
        default_fields.append('week' + str(i))

    return default_fields

def get_all_waterfall_track_fields():
    default_fields = ['Node', 'Label']
    for i in range(1, 14):
        default_fields.append('week' + str(i))

    return default_fields


def validate_waterfall_headers(fields):
    default_fields = get_all_waterfall_fields()

    for field in fields:
        if field not in default_fields:
            return False

    return True

def validate_waterfall_track_headers(fields):
    default_fields = get_all_waterfall_track_fields()
    for field in fields:
        if field not in default_fields:
            return False
    return True
