"""
Django settings for aviso-core project.

This file is kept for backward compatibility.
Settings have been moved to aviso_core/settings/ package.
This file redirects to the settings package.
"""
# Import from the new settings structure (avoid circular import)
import os
from pathlib import Path

# Set default environment if not set
os.environ.setdefault('DJANGO_ENVIRONMENT', 'development')
from aviso.settings import *
# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = os.environ.get('SECRET_KEY', 'django-insecure-change-this-in-production-aviso-core')

# SECURITY WARNING: don't run with debug turned on in production!
# DEBUG = os.environ.get('DEBUG', 'True') == 'True'

# ALLOWED_HOSTS = os.environ.get('ALLOWED_HOSTS', '*').split(',')

# Application definition
INSTALLED_APPS += [
    'rest_framework',

    # Local apps
    'api',
    'config',
    'data_load',
    'domainmodel',
    'fm_service',
    'infra',
    'utils',
    'tasks',
    'deal_service',
    'fake_data',
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
    # 'aviso_core.middleware.SecurityContextMiddleware',  # Custom middleware for security context
]

ROOT_URLCONF = 'aviso_core.urls'


WSGI_APPLICATION = 'aviso_core.wsgi.application'

# Database - MongoDB is used, but Django still needs a database for sessions/auth
# DATABASES = {
#     'default': {
#         'ENGINE': 'django.db.backends.sqlite3',
#         'NAME': BASE_DIR / 'db.sqlite3',
#     }
# }

import os
from urllib.parse import urlparse, unquote


def _parse_database_url(url):
    """Parse a database URL and return a Django DATABASE dict.
    Supports postgres/pgsql and sqlite URLs.
    """
    if not url:
        return None
    parsed = urlparse(url)
    scheme = parsed.scheme
    if scheme.startswith('postgres') or scheme.startswith('postgresql'):
        # path contains the database name (with a leading '/')
        name = parsed.path[1:] if parsed.path and parsed.path != '/' else ''
        # If there's no database name in the URL, consider this invalid so
        # we fall back to the default (usually a local sqlite DB).
        if not name:
            return None
        user = unquote(parsed.username) if parsed.username else ''
        password = unquote(parsed.password) if parsed.password else ''
        host = parsed.hostname or ''
        port = str(parsed.port) if parsed.port else ''
        return {
            'ENGINE': 'django.db.backends.postgresql',
            'NAME': name,
            'USER': user,
            'PASSWORD': password,
            'HOST': host,
            'PORT': port,
        }
    elif scheme == 'sqlite' or url.endswith('.sqlite3') or url == ':memory:':
        # sqlite: either file path or memory
        if scheme == 'sqlite':
            # url like sqlite:////absolute/path or sqlite:///relative/path
            # urlparse gives path directly
            db_path = parsed.path
            if db_path.startswith('/'):
                # absolute path or leading slash for file
                name = db_path
            else:
                name = os.path.join(BASE_DIR, db_path)
        else:
            name = url
        return {
            'ENGINE': 'django.db.backends.sqlite3',
            'NAME': str(name),
        }
    else:
        return None

# Only set DATABASES if not provided by the imported aviso.settings or if NAME missing
try:
    _has_name = isinstance(DATABASES, dict) and DATABASES.get('default') and DATABASES['default'].get('NAME')
except NameError:
    _has_name = False

if not _has_name:
    # Prefer explicit PG_DB_CONNECTION_URL, then postgres_common_db_url, then PG_DB_CONNECTION_URL env var
    db_url = os.environ.get('PG_DB_CONNECTION_URL') or os.environ.get('postgres_common_db_url') or os.environ.get('PG_DATABASE_URL')
    db_settings = _parse_database_url(db_url) if db_url else None
    if not db_settings:
        # fallback to a local sqlite database to ensure NAME exists
        db_settings = {
            'ENGINE': 'django.db.backends.sqlite3',
            'NAME': str(BASE_DIR / 'db.sqlite3'),
        }
    DATABASES = {'default': db_settings}

# Sanitize DATABASES: ensure every configured DB has a NAME. If a PostgreSQL entry has no
# NAME, replace that entry with a local sqlite fallback for that alias. This avoids
# Django raising "Please supply the NAME value" while preserving other valid settings.
for _alias, _cfg in list(DATABASES.items()):
    try:
        engine = _cfg.get('ENGINE', '') if isinstance(_cfg, dict) else ''
    except Exception:
        engine = ''
    if engine and ('postgres' in engine or 'postgresql' in engine):
        name = _cfg.get('NAME') if isinstance(_cfg, dict) else None
        if not name:
            # Create a sqlite fallback specific to this alias so it doesn't overwrite default DB
            fallback_path = str(BASE_DIR / f"{_alias}_fallback.sqlite3")
            DATABASES[_alias] = {
                'ENGINE': 'django.db.backends.sqlite3',
                'NAME': fallback_path,
            }

# Static files (CSS, JavaScript, Images)
STATIC_URL = '/static/'
STATIC_ROOT = BASE_DIR / 'staticfiles'
MEDIA_URL = '/media/'  # this must not be empty
MEDIA_ROOT = os.path.join(BASE_DIR, 'media')
# Default primary key field type
DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

# REST Framework configuration
REST_FRAMEWORK = {
    'DEFAULT_RENDERER_CLASSES': [
        'rest_framework.renderers.JSONRenderer',
    ],
    'DEFAULT_PARSER_CLASSES': [
        'rest_framework.parsers.JSONParser',
        'rest_framework.parsers.FormParser',
        'rest_framework.parsers.MultiPartParser',
    ],
    'DEFAULT_AUTHENTICATION_CLASSES': [
        'rest_framework.authentication.SessionAuthentication',
        # Add custom authentication classes here if needed
    ],
    'DEFAULT_PERMISSION_CLASSES': [
        'rest_framework.permissions.IsAuthenticated',
    ],
    'DEFAULT_PAGINATION_CLASS': 'rest_framework.pagination.PageNumberPagination',
    'PAGE_SIZE': 100,
    'DEFAULT_FILTER_BACKENDS': [
        'rest_framework.filters.SearchFilter',
        'rest_framework.filters.OrderingFilter',
    ],
}

from django.conf import global_settings

# Ensure DEBUG is explicitly set from environment to avoid being overridden by an imported external `aviso.settings`.
# Default to True for local development; allow overriding with the DEBUG env var.
DEBUG = os.environ.get('DEBUG', os.environ.get('DJANGO_DEBUG', 'True')) == 'True'

# Also respect DJANGO_ENVIRONMENT variable: if it contains 'production', default DEBUG to False unless DEBUG env var explicitly set.
if 'DJANGO_ENVIRONMENT' in os.environ and 'production' in os.environ.get('DJANGO_ENVIRONMENT', '').lower():
    # Only force False if user didn't explicitly set DEBUG env var to True
    if 'DEBUG' not in os.environ and 'DJANGO_DEBUG' not in os.environ:
        DEBUG = False

# Ensure ALLOWED_HOSTS is defined and non-empty. Prefer explicit env var, then DEBUG fallback, then Django global settings.
if 'ALLOWED_HOSTS' in globals() and ALLOWED_HOSTS:
    # keep existing value
    pass
else:
    env_allowed = os.environ.get('ALLOWED_HOSTS')
    if env_allowed:
        # support comma-separated list in env var
        ALLOWED_HOSTS = [h.strip() for h in env_allowed.split(',') if h.strip()]
    else:
        if DEBUG:
            ALLOWED_HOSTS = ['*']
        else:
            # In production, if ALLOWED_HOSTS not provided, fall back to Django's global default (empty)
            # This will intentionally be empty and cause Django to complain so operators must set ALLOWED_HOSTS
            ALLOWED_HOSTS = global_settings.ALLOWED_HOSTS
