"""
Main API URL routing.
This file can be used to aggregate all API versions.
"""
from django.urls import path, include
from api.drilldown_fields_v2 import *
from api.data_load import  *
from api.deals_results import *
app_name = 'api'

urlpatterns = [
    path(r'v2/drilldown_fields', DrilldownFieldsV2.as_view(),  name='drilldown_fields_v2'),
    path(r'basic_results', DataLoadAPIView.as_view(), name='basic_results'),
    path(r'deals_results', DealsResultsAPIView.as_view(), name='deals_results'),
]
