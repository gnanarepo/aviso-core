"""
Main API URL routing.
This file can be used to aggregate all API versions.
"""
from django.urls import path, include
from . drilldown_fields_v2 import *

app_name = 'api'

urlpatterns = [
    path('drilldown_fields_v2/', DrilldownFieldsV2.as_view(),  name='drilldown_fields_v2'),
]



