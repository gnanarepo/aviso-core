#!/usr/bin/env python
"""Django's command-line utility for administrative tasks."""
import itertools
if not hasattr(itertools, "izip"):
    itertools.izip = zip
import os
import sys
import zipimport
import importlib.util
from dotenv import load_dotenv

load_dotenv()

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
        # 1. The Magic: Insert the zip path into sys.path
        # This makes __import__('etl_...') work immediately by using standard PathFinder
        if path and path not in sys.path:
            sys.path.insert(0, path)
            print(f"DEBUG: PatchedZipImporter added {path} to sys.path")

        # 2. Call original init so the object behaves normally
        try:
            super().__init__(path)
        except Exception:
            # Fallback: Sometimes the file isn't ready, but we still want the path hook
            pass

    # 3. The Compatibility Fix: Handle Python 3.10+ argument signature
    def find_spec(self, fullname, path=None, target=None):
        # The legacy code adds this importer to sys.meta_path.
        # Python 3.10 calls meta_path finders with (fullname, path, target).
        # The original zipimporter.find_spec doesn't accept 'path', causing the TypeError.

        # ACTION: We return None.
        # REASON: By returning None, we tell Python "skip this finder".
        # Python then moves to the default PathFinder.
        # Since we added the zip to sys.path in __init__, the PathFinder
        # will successfully find and import the module natively.
        return None


# Apply the patch globally
zipimport.zipimporter = PatchedZipImporter

def main():
    """Run administrative tasks."""
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'aviso_core.settings')
    try:
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Couldn't import Django. Are you sure it's installed and "
            "available on your PYTHONPATH environment variable? Did you "
            "forget to activate a virtual environment?"
        ) from exc
    execute_from_command_line(sys.argv)


if __name__ == '__main__':
    main()