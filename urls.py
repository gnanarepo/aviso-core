"""
Main API URL routing.
"""
from django.urls import path
from .api.drilldown_fields_v2 import DrilldownFieldsV2
from .api.data_load import DataLoadAPIView
from .api.deals_results import DealsResultsAPIView

app_name = 'api'

urlpatterns = [
    path('v2/drilldown_fields/', DrilldownFieldsV2.as_view(), name='drilldown_fields_v2'),
    path('basic_results/', DataLoadAPIView.as_view(), name='basic_results'),
    path('deals_results/', DealsResultsAPIView.as_view(), name='deals_results'),
]
