TENANT_NAME = u'tenant.name'
TENANT_USERNAME = u'tenant.username'
LOGIN_TENANT_NAME = u'login.tenant.name'
CSV_VERSION_INFO = u'csv_version_info'
LOGIN_USER_NAME = u'login.user.name'
SWITCH_TYPE = u'login.switch.type'


def request_session_helper(request):
    user_name, tenant_name = request.user.username.split('@', 1)
    request.session['LOGIN_AS'] = request.user.username
    request.session[TENANT_USERNAME] = user_name
    request.session[TENANT_NAME] = tenant_name
    request.session[LOGIN_TENANT_NAME] = tenant_name
    request.session[LOGIN_USER_NAME] = user_name
    return user_name, tenant_name


def is_authenticated(user):
    """GnanaUser exposes is_authenticated as a method, AnonymousUser as a property."""
    flag = getattr(user, 'is_authenticated', False)
    return flag() if callable(flag) else bool(flag)
