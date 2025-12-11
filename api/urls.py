"""
Main API URL routing.
This file can be used to aggregate all API versions.
"""
from django.urls import path, include

app_name = 'api'

urlpatterns = [
    path('v1/', include('api.v1.urls')),
    path('v2/', include('api.v2.urls')),
]

