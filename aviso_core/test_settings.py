"""Settings for the contract tests.

Keeps the real middleware, urls and auth wiring, but puts Django's own tables
in an in-memory sqlite so the suite needs no Postgres.
"""
import tempfile

from aviso_core.settings import *  # noqa: F401,F403
from aviso_core.settings import MIDDLEWARE as _MIDDLEWARE

DEBUG = False

# WhiteNoise only matters when a real server serves /static; the test client
# does not go through it.
MIDDLEWARE = [m for m in _MIDDLEWARE if 'whitenoise' not in m]

STATIC_ROOT = tempfile.mkdtemp(prefix='aviso-core-static-')

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': ':memory:',
    }
}


ALLOWED_HOSTS = ['*']

PASSWORD_HASHERS = ['django.contrib.auth.hashers.MD5PasswordHasher']

LOGGING_CONFIG = None
