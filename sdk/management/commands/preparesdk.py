"""Build the SDK package avisosdk downloads from this service.

Layout expected by avisosdk._fetch_methods (aviso-sdk/avisosdk/__init__.py):

    __package__            json string, the package prefix
    <prefix>/__init__.py
    <prefix>/meta.txt      json list of app names
    <prefix>/<app>/sdk/shell.py   defines shell_methods

The version is a hash of the packaged bytes rather than a git revision: the
image is built with .dockerignore excluding .git, and a content hash also makes
every task agree on the same version without coordinating.
"""
import hashlib
import json
import os
import shutil
import tempfile
import zipfile

from django.conf import settings
from django.core.management.base import BaseCommand

SDK_DIRS = ('sdk', 'interface')


def _collect(app_root, destination):
    """Copy the sdk/ and interface/ trees of an app, preserving structure."""
    for entry in sorted(os.listdir(app_root)):
        path = os.path.join(app_root, entry)
        if os.path.isdir(path):
            if entry in SDK_DIRS:
                shutil.copytree(path, os.path.join(destination, entry),
                                ignore=shutil.ignore_patterns('__pycache__', '*.pyc'))


def _zip_directory(root):
    payload = []
    for base, _dirs, files in os.walk(root):
        for name in sorted(files):
            full = os.path.join(base, name)
            payload.append((os.path.relpath(full, root), full))
    payload.sort()

    digest = hashlib.sha1()
    for arcname, full in payload:
        digest.update(arcname.encode())
        with open(full, 'rb') as handle:
            digest.update(handle.read())
    return payload, digest.hexdigest()[:12]


class Command(BaseCommand):
    help = 'Build the downloadable SDK package and record its version'

    def handle(self, *args, **options):
        app_paths = getattr(settings, 'APP_PATH_DIRS', {})
        if not app_paths:
            self.stderr.write('APP_PATH_DIRS is empty, nothing to package')
            return

        with tempfile.TemporaryDirectory() as staging:
            content = os.path.join(staging, 'content')
            os.makedirs(content)

            packaged = []
            for app, app_root in sorted(app_paths.items()):
                app_dir = os.path.join(content, app)
                os.makedirs(app_dir)
                _collect(app_root, app_dir)
                open(os.path.join(app_dir, '__init__.py'), 'a').close()
                packaged.append(app)

            with open(os.path.join(content, 'meta.txt'), 'w') as handle:
                json.dump(packaged, handle)
            open(os.path.join(content, '__init__.py'), 'a').close()

            payload, digest = _zip_directory(content)
            prefix = '%s_%s' % (settings.AVISO_APPS, digest)

            static_root = settings.STATIC_ROOT
            os.makedirs(static_root, exist_ok=True)
            archive = os.path.join(static_root, '%s.zip' % prefix)

            with zipfile.ZipFile(archive, 'w', zipfile.ZIP_DEFLATED) as bundle:
                bundle.writestr('__package__', json.dumps(prefix))
                for arcname, full in payload:
                    bundle.write(full, os.path.join(prefix, arcname))

        with open(os.path.join(settings.BASE_DIR, settings.SDK_VERSION_FILE_NAME), 'w') as handle:
            handle.write(prefix)

        self.stdout.write('Packaged %s into %s' % (', '.join(packaged), archive))
