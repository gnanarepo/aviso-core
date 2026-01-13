import os
import sys
import json
import time
import logging
from aviso.domainmodel.app import User
from aviso.domainmodel.saml_idp import IdP
from aviso.onelogin.saml import AuthRequest
from aviso.framework.diagnostics import probe_util
from aviso.framework import LoginUserContext, tracer, NewSecContext
from django.contrib.auth.forms import AuthenticationForm
from aviso.settings import CNAME, event_context, sec_context, microservices_user, script_users
from aviso.utils.stringUtils import html_escape
from aviso.utils import is_true, queue_name, is_prod, GnanaError
from django.views import View

logger = logging.getLogger('gnana.%s' % __name__)

class HTTPError(Exception):
    """Base exception for HTTP-like errors"""

    def __init__(self, message, status_code=500):
        self.message = message
        self.status_code = status_code
        super().__init__(message)


class ForbiddenError(HTTPError):
    def __init__(self, message="Forbidden"):
        super().__init__(message, 403)


class NotFoundError(HTTPError):
    def __init__(self, message="Not Found"):
        super().__init__(message, 404)


class RedirectError(Exception):
    """Exception to handle Redirects in flow"""

    def __init__(self, url):
        self.url = url

class ValidationError(Exception):
    """Custom Validation Error to replace Django's ValidationError"""
    pass

class GnanaAuthenticationForm(AuthenticationForm):
    def clean(self):
        username = self.cleaned_data.get("username")
        user_and_tenant = username.split('@')
        if(len(user_and_tenant) == 1):
            raise ValidationError('Invalid username format')
        else:
            user, tenant  = user_and_tenant
            from aviso.domainmodel.tenant import Tenant
            t = Tenant.getByName(tenant)
            if t:
                with NewSecContext(username, tenant, tenant):
                    user = User.getUserByLogin(username)
                    if user and user.account_locked:
                        raise ValidationError("User Account Locked!")
        return super(GnanaAuthenticationForm, self).clean()


# --- Main Code ---

class Role(object):
    """This class is just a place holder to make sure constant names are used
    consistently in the code
    """
    Gnacker = "gnacker"
    DataGenerator = "data-generator"
    Administrator = "administrator"
    User = "user"
    Operator = "operator"
    All_Roles = {"all-users"}


def base_validations(request, **kwargs):
    """
    Refactored validation.
    Expects 'request' to be an object/dict with:
    - path (str)
    - user (object with .username, .is_authenticated())
    - headers (dict) - formerly META
    - params (dict) - formerly GET
    """
    is_protected = kwargs.get('is_protected', False)
    allowed_paths = ['/account/switch',
                     '/account/login', '/account/whoAmI', '/csrfform', '/account/validate_ip', '/account/logout']

    # Logic extracted
    path = getattr(request, 'path', '')
    user = getattr(request, 'user', None)

    # Check if user is authenticated (mocking Django behavior)
    is_authenticated = getattr(user, 'is_authenticated', lambda: False)
    if callable(is_authenticated):
        is_authenticated = is_authenticated()

    username = getattr(user, 'username', '')

    if (path not in allowed_paths and  # check for auth_token once user gets logged in
            sec_context.name != 'administrative.domain' and  # allowing for administrative.domain
            is_authenticated and  # user needs to be authenticated
            username in [microservices_user] + script_users and  # restrictions script users only
            CNAME != 'localhost'):

        # NOTE: In Python Request usually params/query_string is used instead of GET
        sandbox = request.params.get('sandbox', None)
        if sandbox == 'None':
            sandbox = None
        try:
            # Replaced META with headers
            auth_token = request.headers.get('HTTP_ACCESS_TOKEN')
        except:
            auth_token = None

        from aviso.domainmodel.auth_tokens import AuthorizationToken
        if not auth_token:
            logger.info("Failed as no authorized token sent in the header.")
            raise ForbiddenError("Not Authorized")
        else:
            try:
                auth_token_obj = AuthorizationToken.getBySpecifiedCriteria({
                    'object.token': auth_token,
                    "object.source_tenant": sec_context.details.name
                })
            except:
                auth_token_obj = None

            if not auth_token_obj:
                logger.info(
                    "Failing as no authorization token object found in server.")
                raise ForbiddenError("Not Authorized")

            if not auth_token_obj.source_tenant == sec_context.details.name:
                logger.info("Failing because source tenant is %s instead of %s" % (
                    auth_token_obj.source_tenant, sec_context.details.name))
                raise ForbiddenError("Not Authorized")
            if not sandbox and is_protected:
                if not auth_token_obj.primary_tenant:
                    raise ForbiddenError("Trying to change main data, You are not PRIMARY tenant!! :(")


def protect_post(request, **kwargs):
    """This validation is added to the api's which needs to protect from
    unwanted changes to main data even with sandboox"""
    try:
        auth_token = request.headers.get('HTTP_ACCESS_TOKEN')
    except KeyError as e:
        logger.warn("auth token is not available in request")
        auth_token = None

    user = getattr(request, 'user', None)
    is_authenticated = getattr(user, 'is_authenticated', lambda: False)
    if callable(is_authenticated): is_authenticated = is_authenticated()
    username = getattr(user, 'username', '')

    if auth_token or is_authenticated and username in [microservices_user] + script_users and CNAME != 'localhost':
        is_protected = kwargs.get('is_protected', False)
        stage_aware = kwargs.get('stage_aware')

        sandbox = request.params.get('sandbox', None)
        if sandbox == 'None':
            sandbox = None

        from aviso.domainmodel.auth_tokens import AuthorizationToken
        try:
            auth_token_obj = AuthorizationToken.getByFieldValue("token", auth_token)
            is_primary = auth_token_obj.primary_tenant
        except:
            # Probably table is not there!!
            auth_token_obj = None
            is_primary = False

        if request.method == 'POST':
            if not sandbox and is_protected and not is_primary:
                raise ForbiddenError("Trying to change main data, You are not PRIMARY tenant!! :(")
            # data upload/chipotle should not be run from a non primary tenant
            if stage_aware and not is_primary:
                raise ForbiddenError("Trying to change main data, You are not PRIMARY tenant!! :(")


class AvisoView(View):
    '''
    Pure Python implementation of the AvisoView.
    Dependencies on Django View are removed.
    '''
    http_method_names = ['get', 'post']
    login_required = True
    as_json = True
    as_template = None
    async_task = None
    get_template = None
    post_is_get = True
    as_redirect = None
    restrict_to_roles = set()
    Role = Role
    pre_validations = []
    privelege_required_for = set()
    privelege_name = None
    sf_session_checked = False
    negative_privileges = ['read_only_csv']
    is_protected = False
    stage_aware = False

    def __init__(self, *args, **kwargs):
        pass

    def submitasync(self, request, task, *args, **options):
        try:
            context_data = self.get_context_data(request, *args, **options)

            # Check if context_data is a "Response" object (dict in our case)
            if isinstance(context_data, dict) and 'status' in context_data and 'body' in context_data:
                return context_data
            elif not isinstance(context_data, dict):
                raise GnanaError("Unexpected error. Expecting a dictionary")

            options.update(context_data)
            options.update(probe_util.get_probe_context())
            options['profile'] = request.params.get('profile')
            options['debug'] = request.params.get('debug')
            options['trace'] = tracer.trace
            options['event_id'] = event_context.event_id

            wait_for_pool = request.params.get('wait_for_pool', True)

            base_queue_name = getattr(self, 'async_queue', 'master')
            base_queue_name = options.pop('queue', base_queue_name)
            worker_pool = request.params.get('worker_pool', 'primary-pool')
            if worker_pool in ('None', 'default', 'primary', 'main', ""):
                worker_pool = None

            if os.environ.get('WORKER_POOL', None) is None and worker_pool is not None:
                celery_q = '%s_%s' % (worker_pool, base_queue_name)
            else:
                celery_q = queue_name(base_queue_name)
            logger.info("Message will be submitted into worker %s", worker_pool)
            options['pool_name'] = worker_pool

            ret = {}

            user_and_tenant = [sec_context.user_name, sec_context.name,
                               sec_context.login_tenant_name,
                               sec_context.csv_version_info]
            jobid = task(user_and_tenant, *args, **options)

            ret.update({'jobid': jobid, 'trace': tracer.trace})
            ret.update(probe_util.get_probe_context())

            tdetails = sec_context.details
            tdetails.set_flag('last', 'task_submission_time', time.time())
            tdetails.set_flag('last', 'submitted_job_name', task.__name__)

            # Return standard dictionary response
            return {
                "status": 200,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps(ret)
            }
        except GnanaError:
            raise
        except:
            logger.error("Unable to submit the async message.",
                         exc_info=sys.exc_info())
            raise

    def check_results(self, request, model):
        raise NotImplementedError()

    def dispatch(self, request, *args, **kwargs):
        """
        Main entry point. Replaces Django's dispatch logic.
        Expects `request` to be a Python object with:
        - method (str): 'GET', 'POST'
        - params (dict): Query parameters
        - headers (dict): Headers
        - path (str): Path
        - user (obj): User object
        - session (dict): Session storage
        - META (dict): (Legacy support, mapped to headers)
        """

        # Ensure request has META (legacy support for code that checks META)
        if not hasattr(request, 'META'):
            request.META = request.headers

        if request.path == '/sdk/version':
            # In pure python, we just route directly
            if request.method.lower() == 'get':
                return self.get(request, *args, **kwargs)
            return self.post(request, *args, **kwargs)

        hostname = request.headers.get('HTTP_HOST', 'localhost')
        subdomain = hostname.replace('-', '_').split('.')[0]

        sandbox = request.params.get('sandbox', None)
        if sandbox == 'None':
            sandbox = None
        self.request_sandbox = sandbox

        # --- Authentication Check ---
        is_authenticated = getattr(request.user, 'is_authenticated', lambda: False)
        if callable(is_authenticated): is_authenticated = is_authenticated()

        if self.login_required and not is_authenticated:
            if subdomain != CNAME:
                all_tenants = [x for x in IdP.getAll() if x.tenant_name.startswith(
                    subdomain.replace('-', '_') + '.')]

                if len(all_tenants):
                    if len(all_tenants) >= 2:
                        idpmeta = None
                        logger.error('Time to upgrade the software.')
                    else:
                        idpmeta = {}
                        all_tenants[0].encode(idpmeta)
                else:
                    idpmeta = None

                if idpmeta:
                    url = AuthRequest.create(**idpmeta)
                    return {"status": 302, "headers": {"Location": url}, "body": ""}

            request.session['DESTINATION'] = request.path

            if self.as_template:
                form = GnanaAuthenticationForm({
                    "username": request.params.get("username"),
                    "password": request.params.get("password"),
                })
                sso_tenants = [a.tenantName() for a in IdP.getAll()]
                context = {
                    'form': form,
                    'sso': ','.join(sso_tenants),
                    'next': request.path
                }
                # Returning Context directly as we have no Django Templates
                return {"status": 401, "template": 'registration/login.html', "context": context}
            else:
                if self.as_json:
                    # Removed get_csrf_token as it is Django middleware
                    return {
                        "status": 401,
                        "headers": {"Content-Type": "application/json"},
                        "body": json.dumps({'csrf_token': "NotImplementedInPurePython"})
                    }
                else:
                    return {"status": 401, "body": 'Authentication Required'}

        # --- Permission Helper ---
        def check_permission():
            access_allowed = False
            self.user_dimensions = set()
            with LoginUserContext(request.user.username):
                u = User.getUserByLogin(request.user.username)

            for priv in self.privelege_required_for:
                if priv in u.roles and (self.privelege_name in u.roles[priv]):
                    self.user_dimensions.update(
                        list(u.roles[priv][self.privelege_name].keys()))
                    access_allowed = True
                if priv in u.roles and ('*' in u.roles[priv]):
                    list(self.user_dimensions.update(u.roles[priv]['*'].keys()))
                    access_allowed = True
            if self.privelege_name in self.negative_privileges:
                access_allowed = not access_allowed
            return access_allowed

        # --- Authorization Check ---
        if self.login_required:
            self.user_dimensions = '*'
            access_allowed = False

            # Assuming request.user.roles is a set available on the user object
            user_roles = getattr(request.user, 'roles', set())

            if user_roles & (self.restrict_to_roles - self.privelege_required_for):
                access_allowed = True
            else:
                access_allowed = check_permission()

            if not access_allowed and self.privelege_name == "results" and kwargs.get('target', None):
                self.privelege_name = kwargs.get('target') + "_results"
                access_allowed = check_permission()

            if not access_allowed and self.privelege_name == "results" and kwargs.get('model', None):
                results_dimensions = self.check_results(
                    request, kwargs.get('model'))
                if results_dimensions:
                    self.user_dimensions = results_dimensions
                    access_allowed = True

            if not access_allowed:
                logger.error("User do not have any required roles from %s", self.restrict_to_roles)
                return {"status": 403, "body": 'Not Authorized'}

            if self.user_dimensions != '*':
                self.user_dimensions = [x.split('~::~') for x in self.user_dimensions]

        try:
            # --- Validations ---
            for validation in [base_validations] + self.pre_validations:
                kwargs['is_protected'] = self.is_protected
                kwargs['stage_aware'] = self.stage_aware

                try:
                    validation(request, **kwargs)
                except HTTPError as e:
                    return {"status": e.status_code, "body": e.message}

                kwargs.pop('is_protected', None)
                kwargs.pop('stage_aware', None)

            # --- Template GET Processing ---
            if self.get_template and request.method == 'GET':
                data = self.get_context_data(request, *args, **kwargs)
                return {"status": 200, "template": self.get_template, "context": data}

            # --- Async Processing ---
            if (self.async_task and request.method.lower() in self.http_method_names):
                return self.submitasync(request, self.async_task, *args, **kwargs)

            api_profile = is_true(request.params.get('api_profile', False))

            # --- Main Execution (Method Call) ---
            method_func = getattr(self, request.method.lower(), None)
            if not method_func:
                return {"status": 405, "body": "Method Not Allowed"}

            if api_profile:
                import cProfile
                result = {}
                cprofile_filename = get_profile_file_name(self)
                # Note: This is tricky in pure python, keeping logic generic
                cProfile.runctx("result['res'] = method_func(request, *args, **kwargs)",
                                globals(), locals(), filename=get_profile_file_name(self))
                response = result['res']
                # File handling logic remains...
            else:
                response = method_func(request, *args, **kwargs)

        except GnanaError as e:
            if self.get_template:
                data = self.get_context_data(request, *args, **kwargs)
                data['error'] = e.get_error()
                return {"status": 200, "template": self.get_template, "context": data}
            else:
                return e.http_response()

        except Exception as e:
            logger.exception("Unknown exception happened " + str(e))
            if is_prod():
                return {
                    "status": 500,
                    "headers": {"Content-Type": "application/json"},
                    "body": json.dumps({'_error': 'Generic Error occured', 'message': 'Contact System Administrator'})
                }
            else:
                return {
                    "status": 500,
                    "headers": {"Content-Type": "application/json"},
                    "body": json.dumps({'_error': 'Generic Error occured', 'message': html_escape(str(e))})
                }

        # --- Response Standardization ---

        # If response is already a dict with status/body, return it
        if isinstance(response, dict) and 'status' in response:
            return response

        if self.as_redirect:
            return {"status": 302, "headers": {"Location": self.as_redirect}, "body": ""}

        if self.as_template:
            # Without Django, we return the data and template name
            return {"status": 200, "template": self.as_template, "context": response}

        if self.as_json:
            return {
                "status": 200,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps(response)
            }

        logger.error("Unknown type of response received")
        return response

    def get(self, request, *args, **kwargs):
        raise NotFoundError('Not yet implemented')

    def post(self, request, *args, **kwargs):
        if self.post_is_get:
            return self.get(request, *args, **kwargs)
        else:
            raise NotImplementedError()

    def get_context_data(self, request, *args, **kwargs):
        return {}

    def valid_dimension(self, dim_name, dim_value):
        return check_dimension_values(self.user_dimensions, dim_name, dim_value)


def get_profile_file_name(obj):
    return "cp_%s_%s.profile" % (obj.__class__.__name__, time.time())


def check_dimension_values(allowed_dimensions, dim_name, dim_value=None):
    if allowed_dimensions == [['*']] or allowed_dimensions == '*':
        return True

    for allowed_dimension in allowed_dimensions:
        if len(allowed_dimension) == 1:
            allowed_dim_name = allowed_dimension[0]
            if dim_name.startswith(allowed_dim_name):
                return True
            if allowed_dim_name.endswith("*") and len(allowed_dim_name) > 1:
                if dim_name.startswith(allowed_dim_name[:-2]):
                    return True

        if len(allowed_dimension) == 2:
            allowed_dim_name, allowed_dim_value = allowed_dimension
            if dim_name == allowed_dim_name and allowed_dim_value == dim_value:
                return True
            if dim_name.startswith(allowed_dim_name + '~') and dim_value.startswith(allowed_dim_value + '~'):
                return True
            if allowed_dim_name.startswith("*") and len(allowed_dim_name) > 1:
                allowed_dim_key_val = zip(allowed_dim_name[2:].split(
                    "~"), allowed_dim_value[2:].split("~"))
                dim_name_list = dim_name.split("~")
                dim_value_list = dim_value.split("~")
                count = 0
                for d, v in allowed_dim_key_val:
                    if d in dim_name_list and v in dim_value_list:
                        dim_name_index = dim_name_list.index(d)
                        if dim_value_list[dim_name_index] == v:
                            count = count + 1
                if count == len(allowed_dim_key_val):
                    return True
    return False