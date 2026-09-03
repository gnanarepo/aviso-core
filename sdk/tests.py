"""The SDK package contract.

avisosdk.SDKConnection reads the version from the SDK_VERSION response header,
downloads /static/<version>.zip and zipimports it, so those three things are
what is asserted here.

SimpleTestCase, not TestCase: DATABASES is the dummy backend, so anything that
asks for a test database raises ImproperlyConfigured. Nothing here touches the
ORM.
"""
import ast
import json
import os
import sys
import zipfile
import zipimport
from importlib import import_module
from unittest import skipUnless

from django.conf import settings
from django.core.management import call_command
from django.test import SimpleTestCase


class _MetaPathZipImporter(zipimport.zipimporter):
    """Let a zipimporter sit on sys.meta_path under Python 3.10+.

    zipimporter is a path entry finder -- find_spec(fullname, target) -- while
    meta path finders are called with find_spec(fullname, path, target). Up to
    3.9 the import system fell back to the legacy find_module(), whose
    signature happens to match; 3.10 added find_spec() and the mismatch became
    a TypeError. avisosdk._fetch_methods appends a bare zipimporter, so real
    clients need the same widening; scripts/check_sdk_login.py carries a copy.
    """

    def find_spec(self, fullname, path=None, target=None):
        return super().find_spec(fullname, target)


class PrepareSdkTest(SimpleTestCase):

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

    def test_client_framework_is_packaged(self):
        """The verbs need interface/, including the vendored relativedelta."""
        with zipfile.ZipFile(self.archive) as bundle:
            names = bundle.namelist()
        for path in ['gbm_apis/interface/__init__.py',
                     'gbm_apis/interface/base.py',
                     'gbm_apis/interface/shellDateUtils.py',
                     'gbm_apis/interface/relativedelta.py',
                     'gbm_apis/sdk/data.py']:
            self.assertIn('%s/%s' % (self.version, path), names)

    def test_bundle_imports_only_declared_client_packages(self):
        """Every third-party import must be one aviso-sdk installs.

        _fetch_methods re-raises, so an undeclared import here does not merely
        drop the verbs -- it fails connect_sdk() outright. python-dateutil is
        the one that bit us: shellDateUtils now uses the vendored
        gbm_apis/interface/relativedelta.py instead.

        Checked against the import statements rather than the source text,
        because the vendored file's upstream docstring still mentions dateutil.
        """
        declared = {'requests', 'six', 'cryptography', 'pycrypto', 'websocket',
                    'urllib3', 'pytz', 'requests_toolbelt', 'imbox', 'avisosdk'}

        with zipfile.ZipFile(self.archive) as bundle:
            for name in bundle.namelist():
                if not name.endswith('.py'):
                    continue
                for node in ast.walk(ast.parse(bundle.read(name).decode())):
                    if isinstance(node, ast.ImportFrom):
                        if node.level:      # relative, stays inside the bundle
                            continue
                        roots = [node.module.split('.')[0]]
                    elif isinstance(node, ast.Import):
                        roots = [a.name.split('.')[0] for a in node.names]
                    else:
                        continue
                    for root in roots:
                        if root in sys.stdlib_module_names or root in declared:
                            continue
                        self.fail('%s imports %r, which SDK clients do not '
                                  'necessarily have installed' % (name, root))

    def test_bundle_imports_under_python_2(self):
        """SDK clients are a mix of py2 and py3, so the bundle must suit both.

        The py2 gshell checkouts fail with `ImportError: No module named parse`
        on an unguarded `from urllib.parse import ...`, and that fires inside
        _fetch_methods, which re-raises -- so it breaks connect_sdk() outright
        rather than just dropping a verb. Guarded imports sit inside an
        `if six.PY2:` block and so are indented; a module-level one is not.
        """
        py3_only_modules = {'urllib.parse', 'queue', 'configparser', 'io.StringIO'}

        with zipfile.ZipFile(self.archive) as bundle:
            for name in bundle.namelist():
                if not name.endswith('.py'):
                    continue
                tree = ast.parse(bundle.read(name).decode())
                for node in ast.walk(tree):
                    if isinstance(node, ast.ImportFrom) and node.module in py3_only_modules:
                        self.assertGreater(
                            node.col_offset, 0,
                            '%s:%d imports %s unguarded; wrap it in an '
                            'if six.PY2/else block so py2 clients can import it'
                            % (name, node.lineno, node.module))
                    if isinstance(node, ast.JoinedStr):   # an f-string
                        self.fail('%s:%d uses an f-string, which is a py2 '
                                  'SyntaxError' % (name, node.lineno))

    def test_zipimport_yields_shell_methods(self):
        """This is avisosdk._fetch_methods, run against our archive."""
        importer = _MetaPathZipImporter(self.archive)
        sys.meta_path.append(importer)
        self.addCleanup(sys.meta_path.remove, importer)

        package_name = json.loads(importer.get_data('__package__'))
        modules = json.loads(importer.get_data('%s/meta.txt' % package_name))

        methods = {}
        for module in modules:
            module_name = '%s.%s.sdk.shell' % (package_name, module)
            methods.update(getattr(__import__(module_name), module).sdk.shell.shell_methods)

        self.assertIsInstance(methods, dict)
        # The verbs Shell.__init__ and Shell.me() call eagerly during
        # connect_sdk(); without them nobody can reach the service at all.
        self.assertIn('timezone', methods)
        self.assertIn('tenant', methods)
        # Client-side date helpers, and the dataset family.
        self.assertIn('epoch', methods)
        for verb in ['dataset', 'uipmeta', 'sourcemeta']:
            self.assertIn(verb, methods)
        self.assertEqual(methods['uipmeta'].ds_type, 'uip')
        self.assertEqual(methods['sourcemeta'].ds_type, 'source')

    def test_dataset_verb_targets_the_gbm_mount(self):
        """gbm_apis is mounted under gbm/, so the client must ask for /gbm/...

        Guards the pairing that only breaks at runtime: the shell function and
        the endpoint ship together, so a prefix change has to move both.
        """
        importer = _MetaPathZipImporter(self.archive)
        sys.meta_path.append(importer)
        self.addCleanup(sys.meta_path.remove, importer)

        module = __import__('%s.gbm_apis.sdk.data' % self.version)
        data = getattr(module, 'gbm_apis').sdk.data
        self.assertEqual(data.BASE_PATH, '/gbm/dataset')

        # Unregistered commands: DatasetList was not ported and /index_list is
        # not served here, so neither should be reachable.
        commands = data.DatasetPythonSdkFunctions(ds_type='').meta_commands
        self.assertNotIn('list', commands)
        self.assertNotIn('indexes', commands)

    def test_version_is_stable_across_runs(self):
        call_command('preparesdk')
        with open(os.path.join(settings.BASE_DIR, settings.SDK_VERSION_FILE_NAME)) as handle:
            rebuilt = handle.read().strip()
        self.assertEqual(rebuilt, self.version)


def _root_urlconf_imports():
    """Whether the root URLConf can be loaded in this environment.

    accounts/views.py imports aviso.domainmodel.tenant, which imports pyschema
    (pinned at 2.4.0 in requirements.txt). That release is Python 2 only -- its
    core module does `from itertools import izip` -- so on 3.11 the root
    URLConf cannot be built and no view is reachable. Anything using the test
    client is skipped rather than left to fail, or worse hang, on that.
    """
    try:
        import_module(settings.ROOT_URLCONF)
    except Exception:
        return False
    return True


@skipUnless(_root_urlconf_imports(),
            'root URLConf cannot be imported (pyschema 2.4.0 is Python 2 only)')
class SdkEndpointTest(SimpleTestCase):

    def test_version_endpoint_is_open_and_carries_the_header(self):
        response = self.client.get('/sdk/version')

        self.assertEqual(response.status_code, 200)
        self.assertIn('SDK_VERSION', response.headers)
        self.assertEqual(response.json()['version'], response.headers['SDK_VERSION'])
