"""Drive the real avisosdk against a running instance of this service.

Everything else in the suite asserts our side of the protocol. This one hands
the job to the client that will actually be used: its csrf scraping, its cookie
jar, its package download and zipimport. Only the Mongo lookup is stubbed, so
what is left unproven is whether a given user exists there and whether the key
matches -- not whether the conversation works.
"""
from unittest import mock

import avisosdk
from django.core.management import call_command
from django.test import LiveServerTestCase, override_settings

from accounts.tests.test_login_contract import FakeTenant, FakeUser

MICROSERVICES = {'etl_data_service': {'host': 'https://etl-ms.example.com',
                                      'source_tenant': 'aviso.com'}}


class SdkLoginTest(LiveServerTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        call_command('preparesdk')
        # Ask avisosdk for /sdk/latest instead of /static/<version>.zip: the
        # live server does not serve static files. The static path is covered
        # by a request against the real container.
        cls._sdk_debug = avisosdk.sdk_debug
        avisosdk.sdk_debug = True

    @classmethod
    def tearDownClass(cls):
        avisosdk.sdk_debug = cls._sdk_debug
        super().tearDownClass()

    def setUp(self):
        self.user = FakeUser()
        for target in ('authenticate', 'get_user'):
            patcher = mock.patch(
                'accounts.backends.SessionMongoBackend.%s' % target,
                return_value=self.user)
            self.addCleanup(patcher.stop)
            patcher.start()

        # `name` is reserved in the Mock constructor, so it has to be assigned
        app_user = mock.Mock(account_locked=False)
        app_user.name = 'tester'
        patcher = mock.patch('aviso.domainmodel.app.User.getUserByLogin',
                             return_value=app_user)
        self.addCleanup(patcher.stop)
        patcher.start()

        tenant = mock.patch('aviso.domainmodel.tenant.Tenant.getByName',
                            return_value=FakeTenant({'microservices_info': MICROSERVICES}))
        self.addCleanup(tenant.stop)
        tenant.start()

    def test_connect_sdk_downloads_the_package(self):
        shell = avisosdk.connect_sdk(self.live_server_url)

        self.assertNotEqual(shell.version, avisosdk.NOT_DEFINED)
        self.assertIn('aviso_core_', shell.version)

    def test_a_real_shell_logs_in_and_answers_whoami(self):
        shell = avisosdk.connect_sdk(self.live_server_url)
        shell.login_internal('tester@aviso.com', 'secret')

        self.assertIsNotNone(shell._session)

        me = shell.api('/account/whoAmI', None)
        self.assertEqual(me['username'], 'tester@aviso.com')

    def test_the_session_is_reused_across_calls(self):
        """One login, several calls — the cookie has to keep working."""
        shell = avisosdk.connect_sdk(self.live_server_url)
        shell.login_internal('tester@aviso.com', 'secret')

        first = shell.api('/account/whoAmI', None)
        second = shell.api('/account/whoAmI', None)

        self.assertEqual(first['username'], second['username'])
        self.assertEqual(second['current_tenant'], 'aviso.com')

    def test_the_shell_method_that_gates_the_etl_shell(self):
        """python-sdk basic.py:779 makes exactly this call on the gbm shell.

        Not the URL — the packaged shell method, because both halves have to
        line up: a tenant() that builds the right request and a service that
        answers it. A falsy answer here is what silently costs shell.etl.
        """
        shell = avisosdk.connect_sdk(self.live_server_url)
        shell.login_internal('tester@aviso.com', 'secret')

        endpoints = shell.tenant('get_endpoint', endpoint='microservices_info')

        self.assertEqual(endpoints, MICROSERVICES)

    def test_the_shell_survives_the_calls_it_makes_without_a_server(self):
        """Shell.__init__ and the async poller call these before anything else."""
        shell = avisosdk.connect_sdk(self.live_server_url)
        shell.login_internal('tester@aviso.com', 'secret')

        self.assertEqual(shell.tenant(), 'aviso.com')
        # avisosdk calls .lower() on this while polling an async job
        self.assertEqual(shell.tenant('get_flag', category='Datadog',
                                      config_name='enabled', default='True'),
                         'True')
