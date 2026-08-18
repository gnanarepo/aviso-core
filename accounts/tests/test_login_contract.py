"""The contract avisosdk expects from this service.

Every assertion here mirrors something the deployed SDK actually does, so a
change that breaks login shows up as a failing test instead of as a shell that
cannot connect. Mongo and Postgres are mocked; nothing here needs a stack.
"""
import json
import re
from unittest import mock

from django.test import Client, TestCase, override_settings

from gbm_apis.framework.baseView import ValidationError as GnanaValidationError


class FakePk:
    def __init__(self, val):
        self.val = val

    def value_to_string(self, obj):
        return self.val


class FakeUser:
    """Stands in for the GnanaUser the aviso package builds from Mongo."""

    def __init__(self, username='tester@aviso.com'):
        self.username = username
        self.email = username
        self.is_active = True
        self.is_superuser = False
        self.user_timeout = 30
        self.domain = username.rsplit('@', 1)[1]
        self.bson_id = 'deadbeef'
        # GnanaUser.roles is a set, and AvisoView intersects it with a set
        self.roles = {'gnacker', 'all-users'}
        self.last_login = None
        self.saved = False
        self._meta = self

    def save(self, update_fields=None):
        # django.contrib.auth.login() fires update_last_login, which calls this.
        self.saved = True

    @property
    def pk(self):
        return FakePk('%s@%s' % (self.bson_id, self.domain))

    def is_authenticated(self):
        return True

    def has_module_perms(self, module):
        return True

    def has_perm(self, permission):
        return True


class FakeTenant:
    """The tenant document sec_context.details resolves to."""

    def __init__(self, credentials=None, name='aviso.com'):
        self.name = name
        self.credentials = credentials or {}

    def credmap_exist(self, cred_map_name):
        return cred_map_name in self.credentials

    def get_credentials(self, cred_map_name):
        return self.credentials[cred_map_name]

    def get_config(self, category, config_name, default=None):
        return default


class CsrfFormTest(TestCase):

    def test_token_is_readable_by_the_sdk(self):
        """avisosdk.get_csrf_token() parses this with re.search("value='(.*)'").

        Django's {% csrf_token %} switched to double quotes after 1.11, which
        would silently break every SDK login, so the field is rendered by hand.
        """
        response = self.client.get('/csrfform')

        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        match = re.search("value='(.*)'", body)
        self.assertIsNotNone(match, 'SDK regex did not match: %s' % body)
        self.assertTrue(match.group(1))


class AnonymousAccessTest(TestCase):

    def test_health_is_open(self):
        self.assertEqual(self.client.get('/gbm/health/').status_code, 200)

    def test_business_endpoint_rejects_anonymous(self):
        response = self.client.get('/gbm/basic_results?period=2026Q3')
        self.assertEqual(response.status_code, 401)

    def test_business_endpoint_rejects_a_wrong_key(self):
        response = self.client.get('/gbm/basic_results?period=2026Q3',
                                   headers={'internal-api-key': 'not-the-key'})
        self.assertEqual(response.status_code, 401)

    @override_settings()
    def test_unknown_path_is_rejected_before_routing(self):
        self.assertEqual(self.client.get('/no-such-path').status_code, 401)


class LoginTest(TestCase):

    def _patch_backend(self, user):
        patcher = mock.patch(
            'accounts.backends.SessionMongoBackend.authenticate',
            return_value=user)
        self.addCleanup(patcher.stop)
        patcher.start()

        getter = mock.patch(
            'accounts.backends.SessionMongoBackend.get_user',
            return_value=user)
        self.addCleanup(getter.stop)
        getter.start()

    def _patch_form_lookups(self):
        tenant = mock.patch('aviso.domainmodel.tenant.Tenant.getByName',
                            return_value=object())
        self.addCleanup(tenant.stop)
        tenant.start()

        app_user = mock.patch('aviso.domainmodel.app.User.getUserByLogin',
                              return_value=mock.Mock(account_locked=False, name='tester'))
        self.addCleanup(app_user.stop)
        app_user.start()

    def test_rejects_username_without_tenant(self):
        response = self.client.post('/account/login',
                                    {'username': 'tester', 'password': 'x'})
        self.assertEqual(response.status_code, 401)
        self.assertFalse(response.json()['success'])

    def test_opens_a_session_and_records_the_tenant(self):
        user = FakeUser()
        self._patch_backend(user)
        self._patch_form_lookups()

        response = self.client.post('/account/login',
                                    {'username': 'tester@aviso.com',
                                     'password': 'secret'})

        self.assertEqual(response.status_code, 200, response.content)
        self.assertTrue(response.json()['success'])
        self.assertIn('sessionid', response.cookies)
        session = self.client.session
        self.assertEqual(session['tenant.name'], 'aviso.com')
        self.assertEqual(session['login.user.name'], 'tester')

    def test_login_does_not_depend_on_saving_the_user(self):
        """Django's update_last_login signal calls user.save() after login.

        GnanaUser.save() treats a user with no last_login as a first-time
        login and sends a welcome mail whose template this service does not
        ship, so that call raised and the first login of every user came back
        as a 500. The signal is disconnected in accounts/apps.py; this asserts
        the login no longer goes anywhere near save().
        """
        user = FakeUser()
        user.last_login = None
        user.save = mock.Mock(side_effect=AssertionError('save() must not be called'))
        self._patch_backend(user)
        self._patch_form_lookups()

        response = self.client.post('/account/login',
                                    {'username': 'tester@aviso.com',
                                     'password': 'secret'})

        self.assertEqual(response.status_code, 200, response.content)
        user.save.assert_not_called()

    def test_whoami_needs_a_session(self):
        self.assertEqual(self.client.get('/account/whoAmI').status_code, 401)

    def test_login_survives_csrf_the_way_the_sdk_does_it(self):
        """The SDK fetches /csrfform, then posts the token back as X-CSRFToken.

        The default test client skips CSRF entirely, so without this the login
        tests prove nothing about the check the real SDK has to pass.
        """
        user = FakeUser()
        self._patch_backend(user)
        self._patch_form_lookups()

        client = Client(enforce_csrf_checks=True)
        form = client.get('/csrfform', secure=True).content.decode()
        token = re.search("value='(.*)'", form).group(1)

        response = client.post('/account/login',
                               {'username': 'tester@aviso.com',
                                'password': 'secret'},
                               secure=True,
                               headers={'x-csrftoken': token,
                                        'referer': 'https://testserver/'})

        self.assertEqual(response.status_code, 200, response.content)


class TenantEndpointTest(TestCase):
    """``/tenant/<name>/endpoint/microservices_info``.

    get_micro_service_shells() asks this of every service it connects to and
    builds the etl shell only when the answer is non-empty
    (python-sdk basic.py:779-782), so an empty or failing answer here costs the
    caller ``shell.etl`` without any error.
    """

    MICROSERVICES = {'etl_data_service': {'host': 'https://etl-ms.example.com',
                                          'source_tenant': 'aviso.com'}}

    def _login(self, credentials):
        user = FakeUser()
        for target in ('authenticate', 'get_user'):
            patcher = mock.patch(
                'accounts.backends.SessionMongoBackend.%s' % target,
                return_value=user)
            self.addCleanup(patcher.stop)
            patcher.start()

        tenant = mock.patch('aviso.domainmodel.tenant.Tenant.getByName',
                            return_value=FakeTenant(credentials))
        self.addCleanup(tenant.stop)
        tenant.start()

        app_user = mock.patch('aviso.domainmodel.app.User.getUserByLogin',
                              return_value=mock.Mock(account_locked=False))
        self.addCleanup(app_user.stop)
        app_user.start()

        self.client.post('/account/login',
                         {'username': 'tester@aviso.com', 'password': 'secret'})

    def test_needs_a_session(self):
        response = self.client.get('/tenant/aviso.com/endpoint/microservices_info')
        self.assertEqual(response.status_code, 401)

    def test_answers_the_call_that_gates_the_etl_shell(self):
        self._login({'microservices_info': self.MICROSERVICES})

        response = self.client.get('/tenant/aviso.com/endpoint/microservices_info')

        self.assertEqual(response.status_code, 200, response.content)
        self.assertEqual(response.json(), self.MICROSERVICES)

    def test_another_tenant_is_refused(self):
        self._login({'microservices_info': self.MICROSERVICES})

        response = self.client.get('/tenant/someone-else.com/endpoint/microservices_info')

        self.assertEqual(response.status_code, 403)

    def test_no_other_credential_map_is_served(self):
        """The endpoint this is ported from hands back any map, decrypted."""
        self._login({'microservices_info': self.MICROSERVICES,
                     'salesforce': {'password': 'do-not-serve-this'}})

        response = self.client.get('/tenant/aviso.com/endpoint/salesforce')

        self.assertEqual(response.status_code, 404)
        self.assertNotIn(b'do-not-serve-this', response.content)

    def test_an_unconfigured_tenant_gets_an_empty_answer(self):
        """Empty, not 404: a 404 raises inside get_micro_service_shells(), whose
        blanket except would drop shell.gbm back to the app server."""
        self._login({})

        response = self.client.get('/tenant/aviso.com/endpoint/microservices_info')

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {})
class LoginFailureTest(TestCase):
    """The ways login can fail have to reach the client as JSON, not as a 500."""

    def _patch_form(self, side_effect=None):
        patcher = mock.patch(
            'gbm_apis.framework.baseView.GnanaAuthenticationForm.is_valid',
            side_effect=side_effect)
        self.addCleanup(patcher.stop)
        patcher.start()

    def test_a_locked_account_is_a_401(self):
        """baseView raises its own ValidationError, a plain Exception.

        Django's full_clean() only catches django.core.exceptions.ValidationError,
        so this one escapes is_valid() and would surface as a 500 with a
        traceback. avisosdk greps the message for 'locked'.
        """
        self._patch_form(side_effect=GnanaValidationError('User Account Locked!'))

        response = self.client.post('/account/login',
                                    {'username': 'tester@aviso.com',
                                     'password': 'secret'})

        self.assertEqual(response.status_code, 401)
        self.assertIn('locked', response.json()['message'].lower())

    def test_loginswitchbypass_without_a_tenant_is_a_400(self):
        """perform_switch() does `'@' in tenant`, so None was a TypeError —
        raised after the login had already handed out a session."""
        response = self.client.post('/loginswitchbypass',
                                    data=json.dumps({'username': 'tester@aviso.com',
                                                     'password': 'secret'}),
                                    content_type='application/json')

        self.assertEqual(response.status_code, 400)
        self.assertNotIn('sessionid', response.cookies)


class SessionExpiryTest(TestCase):
    """Sessions have to idle out; the service serves no IP challenge."""

    def test_expiry_comes_from_the_user_timeout(self):
        user = FakeUser()
        user.user_timeout = 45
        for target in ('authenticate', 'get_user'):
            patcher = mock.patch(
                'accounts.backends.SessionMongoBackend.%s' % target,
                return_value=user)
            self.addCleanup(patcher.stop)
            patcher.start()
        tenant = mock.patch('aviso.domainmodel.tenant.Tenant.getByName',
                            return_value=object())
        self.addCleanup(tenant.stop)
        tenant.start()
        app_user = mock.patch('aviso.domainmodel.app.User.getUserByLogin',
                              return_value=mock.Mock(account_locked=False))
        self.addCleanup(app_user.stop)
        app_user.start()

        self.client.post('/account/login',
                         {'username': 'tester@aviso.com', 'password': 'secret'})

        self.assertEqual(self.client.session.get_expiry_age(), 45 * 60)


class AccessTokenTest(TestCase):
    """Service-to-service calls carry a token bound to one tenant."""

    def _patch_token(self, tenant_info):
        resolver = mock.patch('accounts.context._resolve_token',
                              return_value=tenant_info)
        self.addCleanup(resolver.stop)
        resolver.start()

        service_user = mock.patch('accounts.context._load_service_user',
                                  return_value=FakeUser('svc@aviso.com'))
        self.addCleanup(service_user.stop)
        service_user.start()

    def test_unknown_token_is_rejected(self):
        self._patch_token(None)
        response = self.client.get('/gbm/basic_results?period=2026Q3',
                                   headers={'access-token': 'nope'})
        self.assertEqual(response.status_code, 401)

    def test_tenant_comes_from_the_token_not_the_header(self):
        self._patch_token({'tenant': 'from-token.com',
                           'source_tenant': 'from-token.com'})
        with mock.patch('accounts.context.sec_context.set_context') as set_context:
            self.client.get('/gbm/basic_results?period=2026Q3',
                            headers={'access-token': 'good',
                                     'x-tenant-name': 'someone-elses.com'})

        self.assertEqual(set_context.call_args.args[1], 'from-token.com')
