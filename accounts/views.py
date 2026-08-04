"""Endpoints avisosdk needs to establish and move a session.

Ported from service-infrastructure/aviso/views.py, trimmed to what an API
service uses: no SSO redirects, no HTML login form, no mixpanel.
"""
import json
import logging

from aviso.domainmodel.app import User as AppUser
from aviso.domainmodel.tenant import Tenant
from aviso.framework import LoginUserContext, NewSecContext
from aviso.settings import ADMIN_DOMAIN, sec_context
from django.contrib.auth import login as auth_login
from django.contrib.auth import logout as auth_logout
from django.http import HttpResponse, JsonResponse
from django.middleware.csrf import get_token
from django.views import View

from accounts.session_utils import (LOGIN_USER_NAME, SWITCH_TYPE, TENANT_NAME,
                                    TENANT_USERNAME, is_authenticated,
                                    request_session_helper)
from gbm_apis.framework.baseView import GnanaAuthenticationForm

logger = logging.getLogger('gnana.%s' % __name__)

DEFAULT_TIME_ZONE = 'America/Los_Angeles'


def _json(payload, status=200):
    return HttpResponse(json.dumps(payload),
                        content_type='application/json',
                        status=status)


def get_timezone(tenant_name=None):
    tenant = Tenant.getByName(tenant_name) if tenant_name else sec_context.details
    return tenant.get_config('forecast', 'timezone', DEFAULT_TIME_ZONE)


class CSRFForm(View):
    """Hand the caller a CSRF token.

    avisosdk scrapes the response with ``re.search("value='(.*)'")``, so the
    field is rendered here rather than through ``{% csrf_token %}`` — Django's
    tag switched to double quotes after 1.11 and the deployed SDK would no
    longer find the token.
    """

    http_method_names = ['get', 'post']

    def get(self, request, *args, **kwargs):
        field = ("<input type='hidden' name='csrfmiddlewaretoken' value='%s' />"
                 % get_token(request))
        return HttpResponse(field, content_type='text/html')

    def post(self, request, *args, **kwargs):
        return self.get(request, *args, **kwargs)


class LoginAjax(View):
    """Validate the credentials and open a session.

    The password may be a real password or the ``SIGNATURE::`` blob the SDK
    produces from the user's private key; both are handled by MongoBackend.
    """

    http_method_names = ['post']

    def post(self, request, *args, **kwargs):
        failure = self.login_check(GnanaAuthenticationForm(data=request.POST), request)
        if failure:
            return failure
        return _json({'success': True,
                      'message': 'User Authentication Successful'})

    def login_check(self, form, request):
        """Return None once the session is open, or the response to send back."""
        form.data = form.data.copy()
        username = (form.data.get('username') or '').lower().strip()
        form.data['username'] = username
        logger.info('|%s| is trying to login', username)

        if not username or '@' not in username:
            logger.error('Incorrect username format.')
            return _json({'success': False,
                          'message': 'User authentication failed'}, status=401)

        if not form.is_valid():
            message = ', '.join(
                msg for errors in form.errors.values() for msg in errors)
            logger.info('Login failed for %s: %s', username, message)
            return _json({'success': False, 'message': message}, status=401)

        auth_login(request, form.get_user())
        request_session_helper(request)
        request.session[SWITCH_TYPE] = 'tenant'
        # No IP second factor on this service; the monolith owns that flow.
        request.session['VALIDIP'] = True
        request.session.save()
        logger.info('Logged in %s', username)
        return None


class Me(View):
    """``/account/whoAmI`` — the SDK reads ``username`` from here after signin."""

    http_method_names = ['get']

    def get(self, request, *args, **kwargs):
        if not is_authenticated(request.user):
            return _json({'success': False, 'message': 'Not authenticated'}, status=401)

        details = {
            'username': request.user.username,
            'email': request.user.email,
            'user_timeout': request.user.user_timeout,
            'current_tenant': sec_context.name,
        }
        with LoginUserContext(request.user.username):
            user = AppUser.getUserByLogin(request.user.username)
            details['name'] = getattr(user, 'name', None)

        try:
            details['timezone'] = str(sec_context.get_tenant_time_zone())
        except Exception:
            details['timezone'] = None

        if sec_context.login_tenant_name != sec_context.name:
            details['switch_type'] = request.session.get(SWITCH_TYPE)
        details['current_user'] = request.session.get(TENANT_USERNAME)
        return JsonResponse(details)


class Logout(View):

    http_method_names = ['get', 'post']

    def get(self, request, *args, **kwargs):
        auth_logout(request)
        return _json({'success': True})

    def post(self, request, *args, **kwargs):
        return self.get(request, *args, **kwargs)


class SSHKeys(View):
    """``/account/keys`` — register the public key that signin() signs with."""

    http_method_names = ['get', 'post']

    def get(self, request, *args, **kwargs):
        if not is_authenticated(request.user):
            return _json({'success': False, 'message': 'Not authenticated'}, status=401)
        with LoginUserContext(request.user.username):
            user = AppUser.getUserByLogin(request.user.username)
        return _json(list(user.ssh_keys.keys()))

    def post(self, request, *args, **kwargs):
        if not is_authenticated(request.user):
            return _json({'success': False, 'message': 'Not authenticated'}, status=401)
        action = request.GET.get('action', 'set')
        with LoginUserContext(request.user.username):
            user = AppUser.getUserByLogin(request.user.username)
            if action == 'set':
                user.set_ssh_pub_key(request.body.decode())
            elif action == 'delete_all':
                user.ssh_keys = {}
            elif action == 'delete':
                key_name = request.GET.get('key', None)
                if not key_name:
                    return _json({'error': 'Missing key name'}, status=400)
                if key_name not in user.ssh_keys:
                    return _json({'error': 'Given key is not found'}, status=404)
                del user.ssh_keys[key_name]
            else:
                return _json(
                    {'error': 'Invalid action. Valid actions are set, delete_all and delete'},
                    status=400)
            user.save()
        return _json(list(user.ssh_keys.keys()))


class Switch(View):
    """``/account/switch`` — move the session to another tenant or identity."""

    http_method_names = ['post']

    def post(self, request, *args, **kwargs):
        if not is_authenticated(request.user):
            return _json({'success': False, 'message': 'Not authenticated'}, status=401)
        if sec_context.login_tenant_name != ADMIN_DOMAIN:
            return _json({'error': 'You are not allowed to switch.'}, status=403)
        try:
            new_identity = json.loads(request.body)
        except Exception:
            return _json({'error': 'Unable to switch'}, status=400)
        return self.perform_switch(new_identity, request)

    def perform_switch(self, new_identity, request):
        with LoginUserContext(request.user.username):
            user = AppUser.getUserByLogin(request.user.username)

        login_as = request.session['LOGIN_AS']
        login_tenant_domain = login_as.split('@')[1]
        impersonation_allowed = False

        if new_identity == 'SELF':
            new_identity = login_as
            logger.info('Switching to original self %s', new_identity)
            impersonation_allowed = True

        if '@' in new_identity:
            return self._switch_identity(request, user, new_identity,
                                         login_as, login_tenant_domain,
                                         impersonation_allowed)
        return self._switch_tenant(request, user, new_identity)

    def _switch_identity(self, request, user, new_identity, login_as,
                         login_tenant_domain, impersonation_allowed):
        new_user_name, new_tenant_name = new_identity.split('@', 1)

        if new_identity in user.linked_accounts:
            impersonation_allowed = True
        elif not impersonation_allowed:
            with LoginUserContext(request.user.username):
                login_user = AppUser.getUserByLogin(login_as)
                admin_roles = login_user.roles.get('administrator', {})

            if 'impersonate' not in admin_roles:
                logger.error('Need impersonate priv in administrator role')
            elif login_tenant_domain == ADMIN_DOMAIN:
                if new_tenant_name in admin_roles['impersonate']:
                    impersonation_allowed = True
                else:
                    logger.error('Tenant must be explicitly assigned for impersonation')
            elif new_tenant_name == login_tenant_domain:
                impersonation_allowed = True
            else:
                logger.error('Impersonation is limited to own tenancy for non admin domain users')

        if not impersonation_allowed:
            return _json({'error': 'You are not allowed to switch to the given identity.'},
                         status=400)

        with NewSecContext(new_user_name, new_tenant_name, login_tenant_domain):
            target_user = AppUser.getUserByLogin(new_identity)
        if not target_user:
            return _json({'error': 'No Such user.'}, status=400)

        request.session[SWITCH_TYPE] = 'user'
        request.session[TENANT_NAME] = new_tenant_name
        request.session[TENANT_USERNAME] = new_user_name
        request.session.pop('csv_version_info', None)
        request.session.save()
        return _json({'success': True})

    def _switch_tenant(self, request, user, new_identity):
        if not any('switch' in roles for roles in user.roles.values()):
            return _json({'error': "You dont have switch privilege."}, status=403)

        switch_tenant = False
        for roles in user.roles.values():
            switch_list = roles.get('switch', [])
            if '-' + new_identity in switch_list:
                return _json({'error': 'This tenant is in your exclusion list.'}, status=403)
            if new_identity in switch_list or '*' in switch_list:
                switch_tenant = True
        if not switch_tenant:
            return _json({'error': 'You cannot switch to this tenant.'}, status=403)

        tenant = Tenant.getByName(new_identity)
        if not tenant:
            return _json({'error': 'Unknown tenant'}, status=400)

        request.session[TENANT_NAME] = tenant.name
        request.session[TENANT_USERNAME] = request.session[LOGIN_USER_NAME]
        request.session[SWITCH_TYPE] = 'tenant'
        request.session.save()
        return _json({'success': True, 'timezone': get_timezone(tenant.name)})


class LoginSwitchByPassCall(LoginAjax, Switch):
    """``/loginswitchbypass`` — what the SDK calls when a tenant is passed to login."""

    http_method_names = ['post']

    def post(self, request, *args, **kwargs):
        try:
            post_data = json.loads(request.body)
        except Exception:
            return _json({'success': False, 'message': 'Invalid request body'},
                         status=400)

        failure = self.login_check(GnanaAuthenticationForm(data=post_data), request)
        if failure:
            return failure
        return self.perform_switch(post_data.get('tenant'), request)


class TenantEndpoint(View):
    """``/tenant/<name>/endpoint/microservices_info`` — where the other services live.

    ``get_micro_service_shells()`` asks this of every service it connects to, and
    builds the etl shell only if the answer is non-empty
    (python-sdk basic.py:779-782). Without it a tenant pointed at this service
    loses ``shell.etl`` silently.

    Only ``microservices_info`` is served. The endpoint it is ported from
    (service-infrastructure tenantmanager.py:746) hands back any credential map
    decrypted, which is a wider door than this service needs.
    """

    http_method_names = ['get']
    endpoint_served = 'microservices_info'

    def get(self, request, tenant_name=None, endpoint_name=None, *args, **kwargs):
        if not is_authenticated(request.user):
            return _json({'success': False, 'message': 'Not authenticated'}, status=401)

        if sec_context.name != tenant_name.lower():
            return _json({'error': 'Unauthorized to get end point information'},
                         status=403)

        if endpoint_name != self.endpoint_served:
            return _json({'error': 'Connector config not found'}, status=404)

        tenant = sec_context.details
        if not tenant.credmap_exist(endpoint_name):
            # Empty rather than the 404 the original returns: a 404 raises inside
            # get_micro_service_shells(), and its blanket except resets shell.gbm
            # back to the app server. An empty answer only skips the etl shell.
            return _json({}, status=200)
        return JsonResponse(tenant.get_credentials(endpoint_name))
