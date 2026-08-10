"""Methods injected into the shell that connects to this service.

avisosdk builds the shell from this package, so anything ``Shell`` itself calls
has to be here. ``Shell.__init__`` calls ``timezone('tenant')`` before a session
exists, and ``Shell.me()`` evaluates ``tenant('get_config', ...)`` eagerly as a
default argument — without both, connect_sdk() raises AttributeError and no one
can reach the service at all.

The endpoints are called through ``shell.api('/gbm/...')``. Named methods for
them land here as the remaining GBM APIs are migrated.
"""
import logging
import os

logger = logging.getLogger(__name__)

DEFAULT_TIME_ZONE = 'America/Los_Angeles'


def _set_local_timezone(shell):
    """Read the client machine's zone.

    shellDateUtils keeps this as a module function and reaches it through
    ``shell.set_local_timezone()``, which only exists on the legacy shell —
    avisosdk.Shell has no such attribute, so it is inlined here.
    """
    if 'TZ' in os.environ:
        shell._tz_name = os.environ['TZ']
        return shell._tz_name
    try:
        shell._tz_name = '/'.join(os.readlink('/etc/localtime').split('/')[-2:])
    except OSError:
        logger.info('Unable to autodetect the local timezone, using UTC')
        shell._tz_name = 'utc'
    return shell._tz_name


def timezone(shell, tz_str, period_info=False):
    """Set the shell's preferred timezone.

    Mirrors aviso/interface/shellDateUtils.py: 'tenant' resolves from the
    logged-in user, and before login it stays unset rather than failing.
    """
    if tz_str == 'local':
        shell._tz_preference = 'local'
        return _set_local_timezone(shell)

    if tz_str != 'tenant':
        shell._tz_preference = tz_str
        shell._tz_name = tz_str
        return shell._tz_name

    shell._tz_preference = 'tenant'
    if hasattr(shell, '_tz_name'):
        del shell._tz_name

    uinfo = shell.me() if getattr(shell, '_session', None) else None
    if not uinfo:
        logger.debug("Until you login, we can't set tenant timezone.")
        shell._tz_name = None
        return shell._tz_name

    shell._tz_name = uinfo.get('timezone') or DEFAULT_TIME_ZONE
    return shell._tz_name


def tenant(shell, action=None, **kwargs):
    """The tenant verb, limited to what the SDK itself calls on this service.

    Mirrors aviso/sdk/tenant.py. Everything else stays unserved until the API
    behind it is migrated.
    """
    if action is None:
        return shell._tenant

    if action == 'get_endpoint':
        # get_micro_service_shells() asks this before it builds the etl shell.
        return shell.api('/tenant/%s/endpoint/%s'
                         % (shell._tenant, kwargs['endpoint']), None)

    if action == 'get_config' and kwargs.get('config_name') == 'timezone':
        return getattr(shell, '_tz_name', None)

    if action == 'get_flag':
        # No flag store here. The SDK calls this while polling an async job and
        # calls .lower() on the answer, so it has to be the default, not None.
        return kwargs.get('default', '')

    logger.info("tenant('%s') is not served by aviso-core", action)
    return None


shell_methods = {
    'timezone': timezone,
    'tenant': tenant,
}
