"""The SDK package contract.

avisosdk.SDKConnection reads the version from the SDK_VERSION response header,
downloads /static/<version>.zip and zipimports it, so those three things are
what is asserted here.
"""
import json
import os
import sys
import zipfile
import zipimport

from django.conf import settings
from django.core.management import call_command
from django.test import TestCase


class PrepareSdkTest(TestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        call_command('preparesdk')
        with open(os.path.join(settings.BASE_DIR, settings.SDK_VERSION_FILE_NAME)) as handle:
            cls.version = handle.read().strip()
        cls.archive = os.path.join(settings.STATIC_ROOT, '%s.zip' % cls.version)

    def test_archive_is_built(self):
        self.assertTrue(os.path.exists(self.archive), self.archive)

    def test_layout_matches_what_the_sdk_reads(self):
        with zipfile.ZipFile(self.archive) as bundle:
            names = bundle.namelist()
            self.assertIn('__package__', names)
            self.assertEqual(json.loads(bundle.read('__package__')), self.version)
            self.assertEqual(json.loads(bundle.read('%s/meta.txt' % self.version)),
                             ['gbm_apis'])
        self.assertIn('%s/gbm_apis/sdk/shell.py' % self.version, names)

    def test_zipimport_yields_shell_methods(self):
        """This is avisosdk._fetch_methods, run against our archive."""
        importer = zipimport.zipimporter(self.archive)
        sys.meta_path.append(importer)
        self.addCleanup(sys.meta_path.remove, importer)

        package_name = json.loads(importer.get_data('__package__'))
        modules = json.loads(importer.get_data('%s/meta.txt' % package_name))

        methods = {}
        for module in modules:
            module_name = '%s.%s.sdk.shell' % (package_name, module)
            methods.update(getattr(__import__(module_name), module).sdk.shell.shell_methods)
        self.assertIsInstance(methods, dict)

    def test_version_is_stable_across_runs(self):
        call_command('preparesdk')
        with open(os.path.join(settings.BASE_DIR, settings.SDK_VERSION_FILE_NAME)) as handle:
            rebuilt = handle.read().strip()
        self.assertEqual(rebuilt, self.version)


class SdkEndpointTest(TestCase):

    def test_version_endpoint_is_open_and_carries_the_header(self):
        response = self.client.get('/sdk/version')

        self.assertEqual(response.status_code, 200)
        self.assertIn('SDK_VERSION', response.headers)
        self.assertEqual(response.json()['version'], response.headers['SDK_VERSION'])
