"""The contract avisosdk expects from this service.

Every assertion here mirrors something the deployed SDK actually does, so a
change that breaks login shows up as a failing test instead of as a shell that
cannot connect. Mongo and Postgres are mocked; nothing here needs a stack.
"""
import re
from unittest import mock

from django.test import TestCase, override_settings


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
        self.roles = {'gnacker': {}}
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

    def test_whoami_needs_a_session(self):
        self.assertEqual(self.client.get('/account/whoAmI').status_code, 401)
