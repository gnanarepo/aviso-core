from django.urls import path

from accounts.views import (CSRFForm, LoginAjax, LoginSwitchByPassCall, Logout,
                            Me, SSHKeys, Switch, TenantEndpoint)

urlpatterns = [
    path('csrfform', CSRFForm.as_view(), name='csrfform'),
    path('tenant/<str:tenant_name>/endpoint/<str:endpoint_name>',
         TenantEndpoint.as_view(), name='tenant_endpoint'),
    path('loginswitchbypass', LoginSwitchByPassCall.as_view(), name='loginswitchbypass'),
    path('account/login', LoginAjax.as_view(), name='account_login'),
    path('account/logout', Logout.as_view(), name='account_logout'),
    path('account/whoAmI', Me.as_view(), name='account_whoami'),
    path('account/switch', Switch.as_view(), name='account_switch'),
    path('account/keys', SSHKeys.as_view(), name='account_keys'),
]
