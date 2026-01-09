"""
WSGI config for aviso_core project.

It exposes the WSGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/5.2/howto/deployment/wsgi/
"""

import os
import sys
import zipimport
import itertools

# ==========================================
# FIX: Monkey-patch zipimporter for Legacy SDK
# ==========================================
class PatchedZipImporter(zipimport.zipimporter):
    def __init__(self, path):
        # Insert zip path into sys.path so PathFinder can load modules normally
        if path and path not in sys.path:
            sys.path.insert(0, path)
            # Avoid print in WSGI unless debugging
            # print(f"DEBUG: Added zip path {path} to sys.path")

        # Call original initializer
        try:
            super().__init__(path)
        except Exception:
            # Safe fallback if zip is not ready
            pass

    # 🔴 CRITICAL FIX FOR PYTHON 3.10+
    def find_spec(self, fullname, path=None, target=None):
        # Returning None skips this finder
        # Python continues with PathFinder, which works
        return None


# Apply the patch BEFORE Django loads anything
zipimport.zipimporter = PatchedZipImporter

# ==========================================
# FIX: Python 3 Compatibility for Legacy Libs
# ==========================================
if not hasattr(itertools, "izip"):
    itertools.izip = zip

if not hasattr(itertools, "ifilter"):
    itertools.ifilter = filter

if not hasattr(itertools, "ifilterfalse"):
    itertools.ifilterfalse = itertools.filterfalse

if not hasattr(itertools, "imap"):
    itertools.imap = map

# ==========================================
# Standard Django WSGI Setup
# ==========================================
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "aviso_core.settings")

from django.core.wsgi import get_wsgi_application

application = get_wsgi_application()
