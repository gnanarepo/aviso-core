"""
ASGI config for aviso_core project.

It exposes the ASGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/5.2/howto/deployment/asgi/
"""

import os

from django.core.asgi import get_asgi_application

# aviso.settings builds gnana_storage at import time from USE_S3; without this it
# falls back to GnanaFileStorage, which has no daily-results support at all.
# setdefault so an explicit USE_S3 in the environment still wins.
os.environ.setdefault("USE_S3", "True")

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'aviso_core.settings')

application = get_asgi_application()
