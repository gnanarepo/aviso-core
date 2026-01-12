"""
WSGI config for aviso_core project.
"""
import itertools
if not hasattr(itertools, "izip"):
    itertools.izip = zip
import os
import sys
import zipimport
import itertools
import importlib.util

if not hasattr(itertools, "izip"):
    itertools.izip = zip

if not hasattr(itertools, "ifilter"):
    itertools.ifilter = filter

if not hasattr(itertools, "ifilterfalse"):
    itertools.ifilterfalse = itertools.filterfalse

if not hasattr(itertools, "imap"):
    itertools.imap = map

pyschema_spec = importlib.util.find_spec("pyschema")

if pyschema_spec and pyschema_spec.submodule_search_locations:
    pyschema_path = pyschema_spec.submodule_search_locations[0]
    core_path = os.path.join(pyschema_path, 'core.py')

    if os.path.exists(core_path):
        spec = importlib.util.spec_from_file_location("core", core_path)
        module = importlib.util.module_from_spec(spec)
        sys.modules["core"] = module
        spec.loader.exec_module(module)

class PatchedZipImporter(zipimport.zipimporter):
    def __init__(self, path):
        if path and path not in sys.path:
            sys.path.insert(0, path)
        try:
            super().__init__(path)
        except Exception:
            pass

    def find_spec(self, fullname, path=None, target=None):
        return None

zipimport.zipimporter = PatchedZipImporter

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "aviso_core.settings")

from django.core.wsgi import get_wsgi_application
application = get_wsgi_application()