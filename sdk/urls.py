from django.urls import path

from sdk.views import GetLatestSDK, SDKVersion

urlpatterns = [
    path('version', SDKVersion.as_view(), name='sdk_version'),
    path('latest', GetLatestSDK.as_view(), name='sdk_latest'),
]
