from django.urls import path, re_path
from gbm_apis.api.drilldown_fields_v2 import DrilldownFieldsV2
from gbm_apis.api.data_load import DataLoadAPIView
from gbm_apis.api.deals_results import DealsResultsAPIView
from gbm_apis.api.health import HealthCheckView
from gbm_apis.dataset.datasetdesigner import DatasetUpdate, DatasetView

app_name = 'gbm_apis'


## TODO: Health Check Implementation
## TODO: TraceId Propagation- Middleware
urlpatterns = [
    path('v2/drilldown_fields/', DrilldownFieldsV2.as_view(), name='drilldown_fields_v2'),
    re_path('basic_results/?$', DataLoadAPIView.as_view(), name='basic_results'),
    path('deals_results/', DealsResultsAPIView.as_view(), name='deals_results'),
    path('health/', HealthCheckView.as_view(), name='health'),
    re_path(r'^dataset/(?P<dataset>\w+)/(?P<sandbox>\w+)/(?P<action>\w+)$',
            DatasetUpdate.as_view(), name='dataset_update'),
    re_path(r'^dataset/(?P<dataset>\w+)/(?P<sandbox>\w+)$',
            DatasetView.as_view(), name='dataset_sandbox'),
    re_path(r'^dataset/(?P<dataset>\w+)$',
            DatasetView.as_view(), name='dataset'),
]