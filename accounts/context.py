"""Resolve the security context for an incoming request.

Two ways in, tried in this order:

1. a Django session — a user who logged in through the SDK or the browser;
2. an ``Access-Token`` header — service-to-service calls, the token is bound to
   one tenant through the AuthorizationToken table.

Ported from service-infrastructure/aviso/framework/middleware.py.
"""
import json
import logging
import threading
import time

from aviso.settings import ADMIN_DOMAIN, microservices_user, sec_context
from django.http import HttpResponse

from accounts.session_utils import (CSV_VERSION_INFO, LOGIN_TENANT_NAME,
                                    LOGIN_USER_NAME, SWITCH_TYPE, TENANT_NAME,
                                    TENANT_USERNAME, is_authenticated,
                                    request_session_helper)

logger = logging.getLogger('gnana.%s' % __name__)

TOKEN_CACHE_TTL = 3600

_service_user = None
_service_user_lock = threading.Lock()
_token_cache = {}
_token_cache_lock = threading.Lock()


def unauthorized(message='Unauthorized'):
    return HttpResponse(content=json.dumps({'error': message}),
                        status=401,
                        content_type='application/json')


def _resolve_token(auth_token):
    """Map an Access-Token to its tenant, cached in process to spare Postgres."""
    now = time.time()
    with _token_cache_lock:
        cached = _token_cache.get(auth_token)
        if cached and cached[1] > now:
            return cached[0]

    from aviso.domainmodel.auth_tokens import AuthorizationToken
    try:
        token_obj = AuthorizationToken.getByFieldValue('token', auth_token)
    except Exception:
        logger.exception('Failed to look up the authorization token')
        token_obj = None
    if not token_obj:
        return None

    tenant_info = {'tenant': token_obj.tenant,
                   'source_tenant': token_obj.source_tenant}
    with _token_cache_lock:
        _token_cache[auth_token] = (tenant_info, now + TOKEN_CACHE_TTL)
    return tenant_info


def _load_service_user(user_part):
    """The microservices user never changes; build it once per process."""
    global _service_user
    if _service_user is not None:
        return _service_user
    with _service_user_lock:
        if _service_user is None:
            from aviso.domainmodel.app import User as AppUser
            from aviso.models import GnanaUser
            sec_context.set_context(user_part, ADMIN_DOMAIN, ADMIN_DOMAIN,
                                    login_user_name=user_part,
                                    switch_type='tenant')
            app_user = AppUser.getUserByLogin(microservices_user)
            if not app_user:
                return None
            _service_user = GnanaUser(app_user)
    return _service_user


def from_session(request):
    """Context for a logged-in user. Tenant comes from the session, never a header."""
    if request.user.username.find('@') <= 0:
        return None

    if SWITCH_TYPE not in request.session:
        request.session[SWITCH_TYPE] = 'tenant'

    if not request.session.get(TENANT_NAME):
        user_name, tenant_name = request_session_helper(request)
        login_tenant_name = tenant_name
    else:
        tenant_name = request.session[TENANT_NAME]
        try:
            login_tenant_name = request.session[LOGIN_TENANT_NAME]
            user_name = request.session[TENANT_USERNAME]
        except KeyError:
            user_name, tenant_name = request_session_helper(request)
            login_tenant_name = tenant_name

    request.session['access_time'] = time.time()

    sec_context.set_context(user_name, tenant_name, login_tenant_name,
                            login_user_name=request.session[LOGIN_USER_NAME],
                            switch_type=request.session[SWITCH_TYPE],
                            csv_version_info=request.session.get(CSV_VERSION_INFO, {}))
    return tenant_name


def from_token(request, auth_token):
    """Context for a service call carrying a per-tenant Access-Token."""
    tenant_info = _resolve_token(auth_token)
    if not tenant_info:
        return None

    user_part = microservices_user.split('@')[0]
    service_user = _load_service_user(user_part)
    if service_user is None:
        logger.error('Service user %s is not configured', microservices_user)
        return None

    sec_context.set_context(user_part,
                            tenant_info['tenant'],
                            tenant_info['source_tenant'],
                            login_user_name=user_part,
                            switch_type='tenant')
    request.user = service_user
    return tenant_info['tenant']


def resolve(request):
    """Return (tenant_name, how) or (None, None) when the caller is anonymous."""
    if is_authenticated(getattr(request, 'user', None)):
        tenant_name = from_session(request)
        if tenant_name:
            return tenant_name, 'session'
        return None, None

    auth_token = request.headers.get('Access-Token')
    if auth_token:
        tenant_name = from_token(request, auth_token)
        if tenant_name:
            return tenant_name, 'token'
        return None, None

    return None, None
