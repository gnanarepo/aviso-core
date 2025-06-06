import base64
import datetime
import json
import logging
import re
import time
from datetime import UTC

import pytz
from aviso.framework import tracer
from aviso.settings import (POOL_PREFIX, WORKER_POOL, adhoc_task_validity,
                            archive_analyticengine_validity, gnana_db2,
                            res_archive_analyticengine_validity,
                            res_archive_validity,
                            res_cache_analyticengine_validity,
                            res_cache_validity, sec_context,
                            task_expire_seconds, task_validity,
                            taskactive_analyticengine_validity,
                            taskactive_validity, archive_task_validity)
from celery import current_task
from Crypto.Hash import SHA, SHA512
from Crypto.PublicKey import RSA
from Crypto.Signature import PKCS1_PSS

from domainmodel import Model
from utils import date_utils
from utils.common import ip_match
from utils.string_utils import random_string

logger = logging.getLogger('gnana.%s' % __name__)

class AccountEmails(Model):

    """stores information about all incoming/outgoing
    emails for an account id
    """

    collection_name = 'account_emails'
    kind = "domainmodel.app.AccountEmails"
    encrypted = True
    tenant_aware = True
    version = 1.0
    index_list = {
        'account_id': {'unique': True},
    }
    postgres = True
    id_field = 'account_id'

    def __init__(self, attrs=None):
        self.account_id = None
        self.emails = []
        super(AccountEmails, self).__init__(attrs)

    def encode(self, attrs):
        attrs['account_id'] = self.account_id
        if self.postgres:
            attrs['emails'] = json.dumps(self.emails)
        else:
            attrs['emails'] = self.emails

    def decode(self, attrs):
        self.account_id = attrs['account_id']
        if self.postgres:
            self.emails = json.loads(attrs['emails'])
        else:
            self.emails = attrs['emails']

    def set_account_id_and_emails(self, account_id, emails):
        self.account_id = account_id
        self.emails = emails

    @classmethod
    def get_email_by_account_id(cls, account_id):
        return cls.getByFieldValue('account_id', account_id)

    @classmethod
    def create_postgres_table(self):
        table_name = str(self.getCollectionName()).replace(".", "$")
        schema = 'create table "{table_name}"(\
                  _id                       bigserial primary key,\
                  _version                  int,\
                  _kind                     varchar,\
                  last_modified_time        bigint,\
                  account_id              varchar,\
                  emails                     varchar,\
                  _encdata                 varchar\
                 )'

        return gnana_db2.postgres_table_creator(schema.format(table_name=table_name))


class UserError(Exception):

    def __init__(self, error):
        self.error = error


class TaskError(Exception):

    def __init__(self, error):
        self.error = error


class User(Model):
    """ Stores and retrieves user information in a tenant
    specific collection"""
    collection_name = 'user'
    version = 3
    postgres = True
    kind = "domainmodel.app.User"
    tenant_aware = True
    index_list = {
        'username': {'unique': True}
    }
    # Prevents this object from being encrypted.
    encrypted = False
    USER_ROLES = (
        (None, 'Unknown'),
        ('ceo', 'CEO'),
        ('cfo', 'CFO'),
        ('cro', 'CRO'),
        ('customer_success', 'Customer Success'),
        ('sales_regional_vp', 'Sales Regional VP'),
        ('sales_vp_director', 'Sales VP / Director'),
        ('sales_manager', 'Sales Manager'),
        ('sales_ops_vp_director', 'Sales Ops VP / Director'),
        ('sales_ops_manager', 'Sales Ops Manager'),
        ('sales_ops', 'Sales Ops'),
        ('sales_rep', 'Sales Rep'),
        ('finance_vp', 'Finance VP'),
        ('finance_manager', 'Finance  Manager'),
        ('finance_analyst', 'Finance Analyst'),
        ('it', 'IT')
    )

    def __init__(self, attrs=None):

        # Original attributes
        self.name = None
        self.username = None
        self.email = None
        self.secondary_emails = []
        self.app_modules = []
        self.password_hash = None
        self.last_login = None

        self.valid_ips = {}
        self.valid_devices = {}

        # the above field would save the device
        # user tried to login from the validation code for the
        # device and the time at which he tried to login
        self.pwd_validation_code = None
        # datetime.datetime.now()
        self.pwd_validation_expiry = date_utils.now()
        self.roles = {}
        self.account_locked = False
        self.login_attempts = 0
        self.failed_login_time = 0

        self.edit_dims = None

        # Added to allow users to have long lived sessions
        tenant_details = sec_context.details
        self.user_timeout = tenant_details.get_config('security', 'session_timeout', 120)

        # Added to distinguish users created for customers
        # and for gnana to introspect results

        # True if the user is a customer false if the user is a gnana employee.
        self.is_customer = True
        self.user_role = None

        # Additional attrbibutes for new functionality
        self.is_disabled = False
        self.preset_validation_code = None
        self.show_experimental = False
        self.password_salt = None

        self.ssh_keys = {}
        # Added to validate new user through url
        self.uuid = None
        self.uuid_validation_expiry = None
        self.activation_status = None
        self.mail_date = None
        self.is_second_login = False
        # Used only for SSO users
        self.linked_accounts = []
        self.linked_to = ""
        self.refresh_token = ""
        self.user_id = ""
        # [{'device_id': '234', 'token': '92w'}]
        self.notification_tokens = []  # stores users firebase tokens for sending mobile notifications
        self.email_token = ""
        super(User, self).__init__(attrs)

    def set_ssh_pub_key(self, key_value):
        content = key_value.split()
        # Create the key to validate it
        RSA.importKey(' '.join(content))
        self.ssh_keys[content[-1]] = key_value

    def get_code(self):
        if(self.pwd_validation_code and
           self.pwd_validation_expiry > date_utils.now()):
            return self.pwd_validation_code
        return None

    def set_code(self, code):
        self.pwd_validation_code = code
        self.pwd_validation_expiry = (date_utils.now() +
                                      datetime.timedelta(days=1))

    validation_code = property(get_code, set_code)

    def add_devices(self, device):
        time_now = time.time()
        first_record = {
            'first_used': time_now,
            'user_agent': 'Administrative Addition',
        }

        print(f'Current Device is {device}')

        if device not in self.valid_devices:
            self.valid_devices[device] = {}
        self.valid_devices[device].update(first_record)

    def add_notification_token(self, device_id, token):
        device_id = device_id if isinstance(device_id, dict) else {"identifier":device_id}
        if not self.notification_tokens:
            self.notification_tokens = []
        for idx, device_token in enumerate(self.notification_tokens):
            if device_token['device_id'] == device_id:
                device_token['token'] = token
                self.notification_tokens[idx] = device_token
                break
        else:
            self.notification_tokens.append({'device_id': device_id,
                                             'token': token})

    def remove_notification_token(self, device_id):
        if not self.notification_tokens:
            raise Exception('No devices to delete')
        # If device_id is not there raise exception
        device_id_str = device_id["identifier"] if isinstance(device_id, dict) else device_id
        for idx, device_token in enumerate(self.notification_tokens):
            device_token_str = device_token["device_id"]["identifier"] if isinstance(device_token["device_id"], dict) else device_token["device_id"]
            if device_id_str == device_token_str:
                del self.notification_tokens[idx]

    def reset_all_devices(self):
        self.valid_devices = {}

    def remove_device(self, device):
        try:
            return self.valid_devices.pop(device)
        except:
            return False

    def add_ip(self, ip):

        # Check the format
        expression = re.compile('^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}(/[\d]+)?$')
        if not expression.match(ip):
            return False

        first_record = {
            'first_used': time.time(),
            'user_agent': 'Administrative Addition'
        }

        if ip not in self.valid_ips:
            self.valid_ips[ip] = {}
        self.valid_ips[ip].update(first_record)
        return True

    def reset_all_ips(self):
        self.valid_ips = {}

    def remove_ip(self, ip):
        try:
            return self.valid_ips.pop(ip)
        except:
            return False

    def new_validation(self, device, ip, user_agent):
        time_now = time.time()
        first_record = {
            'first_used': time_now,
            'first_used_from': ip
        }
        if device not in self.valid_devices:
            self.valid_devices[device] = {}
        self.valid_devices[device].update(first_record)

        if not 'user_agent' in self.valid_devices[device]:
            self.valid_devices[device]['user_agent'] = user_agent

        ip_used = ip_match(ip, self.valid_ips)

        if ip_used is None:
            tdetails = sec_context.details
            ip_factors = tdetails.get_config('security', 'ip_factors', {})
            strict_ip_check = ip_factors.get('strict_ip_check', True)
            if strict_ip_check:
                ip_used = ip
            else:
                ip_segments = ip.split('.')
                ip_segments[-1] = '0'
                ip_used = '.'.join(ip_segments) + '/24'

        if ip_used not in self.valid_ips:
            self.valid_ips[ip_used] = {}

        self.valid_ips[ip_used].update(first_record)
        if not 'user_agent' in self.valid_ips[ip_used]:
            self.valid_ips[ip_used]['user_agent'] = user_agent

    def set_last_login(self, device, ip, user_agent):
        time_now = time.time()
        last_record = {'last_used': time_now, 'last_used_from': ip}
        if device in self.valid_devices:
            self.valid_devices[device].update(last_record)

        ip_used = ip_match(ip, self.valid_ips)
        if ip_used in self.valid_ips:
            self.valid_ips[ip_used].update(last_record)

    def is_valid_ip(self, ip):
        return ip_match(ip, self.valid_ips.keys())

    def is_valid_device(self, device):
        return device in self.valid_devices

    def get_refresh_token(self):
        return self.refresh_token

    def encode(self, attrs):
        attrs['name'] = self.name
        if self.username:
            attrs['username'] = self.username.lower()
        else:
            raise UserError("Missing username")
        if self.email:
            attrs['email'] = self.email
        else:
            raise UserError("Missing Email")

        if self.secondary_emails:
            attrs['secondary_emails'] = self.secondary_emails
        else:
            attrs['secondary_emails'] = []

        if self.app_modules:
            attrs['app_modules'] = self.app_modules
        else:
            attrs['app_modules'] = []

        if self.password_hash:
            attrs['password'] = self.password_hash
        else:
            # To support postgres update
            attrs['password'] = None
        if self.last_login:
            attrs['last_login'] = self.last_login
        else:
            attrs['last_login'] = None

        if self.validation_code:
            attrs['validation_code'] = self.pwd_validation_code
            attrs['validation_expiry'] = self.pwd_validation_expiry
        else:
            attrs['validation_code'] = None
            attrs['validation_expiry'] = None

        # We are flatting the priv and dim assignment to avoid MongoDB
        # restrictions
        new_roles = {}
        for r, role_val in self.roles.items():
            new_roles[r] = []
            for priv_val in role_val.itervalues():
                new_roles[r].extend(priv_val.values())
        attrs['roles'] = new_roles
        attrs['user_timeout'] = self.user_timeout
        attrs['is_customer'] = self.is_customer
        attrs['user_role'] = self.user_role
        attrs['edit_dims'] = self.edit_dims

        attrs['is_disabled'] = self.is_disabled
        attrs['preset_validation_code'] = self.preset_validation_code
        attrs['password_salt'] = self.password_salt
        attrs['show_experimental'] = self.show_experimental
        attrs['account_locked'] = self.account_locked
        attrs['login_attempts'] = self.login_attempts
        attrs['failed_login_time'] = self.failed_login_time
        attrs['ssh_keys'] = self.ssh_keys.items() if not self.postgres else self.ssh_keys
        if self.uuid:
            attrs['uuid'] = self.uuid
            attrs['uuid_validation_expiry'] = self.uuid_validation_expiry
            attrs['activation_status'] = self.activation_status
        else:
            attrs['uuid'] = None
            attrs['uuid_validation_expiry'] = None
            attrs['activation_status'] = None

        attrs['valid_devices'] = self.valid_devices
        attrs['valid_ips'] = {'items': [(k, v) for k, v in self.valid_ips.items()]}

        if self.mail_date:
            attrs['mail_date'] = self.mail_date
        else:
            attrs['mail_date'] = None
        attrs['is_second_login'] = self.is_second_login
        attrs["linked_accounts"] = self.linked_accounts
        attrs['refresh_token'] = self.refresh_token
        attrs['user_id'] = self.user_id
        attrs['notification_tokens'] = self.notification_tokens
        attrs['email_token'] = self.email_token
        if self.linked_to:
            attrs["linked_to"] = self.linked_to
        return super(User, self).encode(attrs)

    def decode(self, attrs):
        self.name = attrs.get('name')
        self.user_timeout = attrs.get('user_timeout', 120)
        self.username = attrs.get('username')
        self.password_hash = attrs.get('password')
        self.email = attrs.get('email')
        self.secondary_emails = attrs.get('secondary_emails',[])
        self.app_modules = attrs.get('app_modules', [])
        self.last_login = attrs.get('last_login')
        if self.last_login:
            self.last_login = pytz.utc.localize(self.last_login)
        self.pwd_validation_code = attrs.get('validation_code')
        self.pwd_validation_expiry = attrs.get('validation_expiry')
        if self.pwd_validation_expiry:
            self.pwd_validation_expiry = pytz.utc.localize(self.pwd_validation_expiry)
        self.roles = {}

        # Read the roles back and put them in the right place
        new_roles = attrs.get('roles', {})
        for r, flat_list in new_roles.items():
            self.roles[r] = {}
            for priv_tuple in flat_list:
                priv, dim, write, delegate = priv_tuple
                if priv not in self.roles[r]:
                    self.roles[r][priv] = {}
                self.roles[r][priv][dim] = [priv, dim, write, delegate]

        self.edit_dims = attrs.get('edit_dims')
        self.is_customer = attrs.get('is_customer')
        self.user_role = attrs.get('user_role')
        self.is_disabled = attrs.get('is_disabled', False)
        self.preset_validation_code = attrs.get('preset_validation_code')
        self.password_salt = attrs.get('password_salt', None)
        self.show_experimental = attrs.get('show_experimental', False)
        self.account_locked = attrs.get('account_locked', False)
        self.login_attempts = attrs.get('login_attempts', 0)
        self.failed_login_time = attrs.get('failed_login_time', 0)
        self.ssh_keys = dict(attrs.get('ssh_keys', []))
        self.uuid = attrs.get('uuid')
        self.uuid_validation_expiry = attrs.get('uuid_validation_expiry')
        self.activation_status = attrs.get('activation_status')
        self.mail_date = attrs.get('mail_date', None)
        self.is_second_login = attrs.get('is_second_login', False)
        self.linked_accounts = attrs.get("linked_accounts") or []
        self.linked_to = attrs.get("linked_to", "")
        self.refresh_token = attrs.get('refresh_token', "")
        self.user_id = attrs.get('user_id')
        self.notification_tokens = attrs.get('notification_tokens')
        self.email_token = attrs.get('email_token')

        tdetails = sec_context.details
        now = time.time()

        def load_last_n(name, config_name):
            device_factors = tdetails.get_config('security', config_name, {})
            hard_limit = device_factors.get('hard_limit', 60) * 24 * 60 * 60
            soft_limit = device_factors.get('soft_limit', 15) * 24 * 60 * 60
            max_devices = device_factors.get('max_entries', 20)

            valid_device_list = []
            admin_list = []
            attr_value = attrs.get(name, None)
            if not attr_value:
                return {}

            # Postgres workaround to add items
            if 'items' in attr_value:
                attr_value = attr_value['items']

            if isinstance(attr_value, dict):
                attr_value = attr_value.items()

            for d, d_info in attr_value:
                if d_info.get('user_agent', '') == 'Administrative Addition':
                    admin_list.append([None, d, d_info])
                elif now < d_info.get('first_used', 0) + hard_limit and now < d_info.get('last_used', 0) + soft_limit:
                    valid_device_list.append([d_info.get('last_used', 0), d, d_info])

            # Trim the list to first max_devices
            valid_device_list = sorted(valid_device_list, reverse=True)
            if len(valid_device_list) > max_devices:
                valid_device_list = valid_device_list[0:max_devices]
            return dict((x[1], x[2]) for x in admin_list + valid_device_list)

        # Load the valid devices and ips with a limit of last max_devices
        self.valid_devices = load_last_n('valid_devices', 'device_factors')
        self.valid_ips = load_last_n('valid_ips', 'ip_factors')

        super(User, self).decode(attrs)

    def upgrade_attrs(self, attrs):
        if attrs['_version'] < 2:
            attrs['object']['roles'] = dict((k, {}) for k in attrs['object']['roles'])
        if attrs['_version'] < 3:
            new_roles = {}
            for r, role_val in attrs['object'].get('roles', {}).items():
                new_roles[r] = []
                for priv_val in role_val.itervalues():
                    new_roles[r].append(priv_val)
            attrs['object']['roles'] = new_roles

    @classmethod
    def getUserByLogin(cls, username):
        return cls.getByFieldValue('username', username.lower())

    @classmethod
    def getUserByKey(cls, key):
        return cls.getByKey(key)

    @classmethod
    def get_by_user_id(cls, user_id):
        return cls.getByFieldValue('user_id', user_id)

    def verify_password(self, password):
        password = password.strip()
        if password.startswith('SIGNATURE::'):
            dummy, dummy, t, dummy, signature = password.split(':')
            for k in self.ssh_keys.itervalues():
                try:
                    key = RSA.importKey(k)
                    h = SHA.new()
                    h.update(t)
                    verifier = PKCS1_PSS.new(key)
                    if verifier.verify(h, base64.b64decode(signature)):
                        if abs(time.time() - float(t)) < 20:
                            return True
                        else:
                            logger.error('Trying to login using old signature for %s', self.username)
                            return False
                except:
                    logger.exception('Unknown error')
            return False
        else:
            cipher = SHA512.new(password + self.password_salt if self.password_salt else password)
            hashed_pwd = cipher.hexdigest()
            return hashed_pwd == self.password_hash

    def set_password(self, password):
        password = password.strip()
        if password.startswith('SIGNATURE::'):
            raise UserError('Password may not start with that prefix')
        self.password_salt = random_string()
        cipher = SHA512.new(password + self.password_salt)
        self.password_hash = cipher.hexdigest()

    def get_password(self):
        raise UserError("Password can't be retrieved")

    password = property(get_password, set_password)

    def has_password(self):
        return True if self.password_hash else False

    @property
    def user_role_label(self):
        try:
            USER_ROLES = sec_context.details.get_config(category='forecast',
                                           config_name='tenant').get('user_roles', None) or User.USER_ROLES
        except:
            USER_ROLES = User.USER_ROLES
        return dict(USER_ROLES).get(self.user_role)



TASK_STATUS_STRS = {
    0: 'CREATED',
    1: 'STARTED',
    2: 'FINISHED',
    3: 'ERROR',
    4: 'SUBMITTED',
    5: 'TERMINATED',
    6: 'REVOKED' }


class TaskActive(Model):
    collection_name = 'task_active'

    encrypted = False
    tenant_aware = True
    read_from_primary=True

    kind = 'domainmodel.app.TaskActive'
    version = 1
    index_list = {
        'expires': {
            'expireAfterSeconds': 7200
        },
        'extid': {},
    }

    def __init__(self, attrs=None):
        self.extid = None
        self.deps = []
        self.pool_name = 'primary-pool'
        self.pinned = False
        self.primary_dep = None
        self.cached = False
        self.level = None
        self.consumers = []
        self.build_id = None
        self.cname = POOL_PREFIX
        self.expire()
        super(TaskActive, self).__init__(attrs)

    def encode(self, attrs):
        attrs['extid'] = self.extid
        attrs['deps'] = self.deps
        attrs['build_id'] = self.build_id
        attrs['pool_name'] = self.pool_name
        attrs['pinned'] = self.pinned
        attrs['cached'] = self.cached
        attrs['primary_dep'] = self.primary_dep
        attrs['level'] = self.level
        attrs['consumers'] = self.consumers
        attrs['cname'] = self.cname

    def decode(self, attrs):
        self.extid = attrs['extid']
        self.deps = attrs['deps']
        self.build_id = attrs.get('build_id', None)
        self.pool_name = attrs['pool_name']
        self.pinned = attrs.get('pinned', False)
        self.cached = attrs.get('cached', False)
        self.primary_dep = attrs.get('primary_dep', None)
        self.level = attrs.get('level', None)
        self.consumers = attrs.get('consumers', [])
        self.cname = attrs.get('cname', POOL_PREFIX)

    @staticmethod
    def register_active_task(v2task, parent_id, save=True, build_id=None):
        t = TaskActive()
        t.pinned = v2task.is_pinned()
        t.extid = v2task.task_id
        t.deps = list(set(v.res_id for _k, v in v2task.dependencies.items()))
        pool_name = v2task.params.get('pool_name', None) or v2task.context.get('pool_name', None)
        t.pool_name = pool_name if pool_name is not None else WORKER_POOL
        t.cname = POOL_PREFIX
        t.cached = v2task.is_cached()
        t.build_id = build_id
        if t.pinned:
            for _key, task in v2task.dependencies.items():
                if task.is_cached():
                    t.primary_dep = task.res_id
                    break
        elif t.cached:
            t.primary_dep = v2task.res_id
        else:
            t.primary_dep = None
        t.level = v2task.level
        t.consumers = v2task.consumers
        if parent_id not in t.consumers and parent_id != v2task.task_id:
            t.consumers.append(parent_id)
        if 'analyticengine' in v2task.path:
            t.expires = date_utils.now() + datetime.timedelta(taskactive_analyticengine_validity)
        elif 'SnapshotSegmentSnapshotTask' in v2task.path or \
             'snapshot_task' in v2task.path or \
             'DealChangesTotalsTask' in v2task.path or \
             'deal_changes_totals' in v2task.path or \
             'SnapshotAccountsTask' in v2task.path or \
             'snapshot_accounts_task' in v2task.path or \
             'FilterTotalsMainTask' in v2task.path or \
             'filter_totals_task' in v2task.path:
            t.expires = date_utils.now() + datetime.timedelta(adhoc_task_validity)
        if save:
            t.save()
        return t

    def add_consumer(self, parent_id):
        self.consumers.append(parent_id)
        self.save()

    def expire(self):
        self.expires = date_utils.now() + datetime.timedelta(taskactive_validity)


class ResultCache(Model):
    collection_name = 'result_cache'
    tenant_aware = True
    encrypted = False
    read_from_primary=True
    WAITING = 0
    FAILED = 1
    AVAILABLE = 2

    kind = 'domainmodel.app.ResultCache'
    version = 1
    index_list = {
        'expires': {
            'expireAfterSeconds': 7200
        },
        'extid': {},
        'status': {},
        'requesting_task': {},
        'cache_key': {},
        'as_of_mnemonic': {},
        'path': {},
    }

    def __init__(self, attrs=None):
        self.extid = None
        self.expire()
        #  Available, Failed, Waiting
        self.status = None
        self.lifetime = 'short'
        self.requesting_task = None
        self.failed_tasks = []
        self.path = None
        self.cache_key = None
        self.as_of_mnemonic = None
        self.deps = []
        self.filesize = {'compressed': 0,
                         'uncompressed': 0}
        self.result = {}
        self.cached = True

        super(ResultCache, self).__init__(attrs)

    def encode(self, attrs):
        attrs['extid'] = self.extid
        attrs["expires"] = self.expires
        attrs['lifetime'] = self.lifetime
        attrs['status'] = self.status
        attrs['requesting_task'] = self.requesting_task
        attrs['failed_tasks'] = self.failed_tasks
        attrs['path'] = self.path
        attrs['cache_key'] = self.cache_key
        attrs['as_of_mnemonic'] = self.as_of_mnemonic
        attrs['deps'] = self.deps
        attrs['filesize'] = self.filesize
        attrs['result'] = self.result
        attrs['cached'] = self.cached

    def decode(self, attrs):
        self.extid = attrs['extid']
        self.lifetime = attrs.get('lifetime', 'short')
        if 'expires' in attrs:
            self.expires = attrs["expires"]
        else:
            self.expires = None
        self.status = attrs.get('status', 'Created')
        self.requesting_task = attrs.get('requesting_task')
        self.failed_tasks = attrs.get('failed_tasks')
        self.path = attrs.get('path')
        self.cache_key = attrs.get('cache_key', None)
        self.as_of_mnemonic = attrs.get('as_of_mnemonic')
        self.deps = attrs.get('deps')
        self.filesize = attrs.get('filesize', {'compressed': '0', 'uncompressed': 0})
        self.result = attrs.get('result', {})
        self.cached = attrs.get('cached', True)

    @classmethod
    def getByKey(cls, extid):
        return cls.getByFieldValue('extid', extid)

    def expire(self):
        self.expires = datetime.datetime.now(UTC) + datetime.timedelta(res_cache_validity)

    def extend_expire(self):
        # extend the TTL of this cache result if it's used as a result tree.
        # only extend it if the cache life of this object is already 1/2 over however

        expires_in = (self.expires - datetime.datetime.now(UTC)).total_seconds()
        fuzzy_window = 300  #  five 5 mins so it does not update continuously while creating a new tree.
        half_of_cache_time = datetime.timedelta(res_cache_validity).total_seconds()/2

        if (expires_in + fuzzy_window) < half_of_cache_time:
            self.expire()
            self.save(is_partial=True, field_list=["object.expires"])

    @classmethod
    def add_result(cls, v2task):
        shell = cls()
        shell.lifetime = v2task.lifetime
        shell.status = cls.WAITING
        shell.extid = v2task.res_id
        shell.requesting_task = v2task.task_id
        shell.path = v2task.path
        shell.cache_key = v2task.cache_key
        shell.as_of_mnemonic = v2task.as_of_mnemonic
        shell.deps = list(set(v.res_id for k, v in v2task.dependencies.items()))
        shell.cached = v2task.is_cached()
        if 'analyticengine' in v2task.path:
            shell.expires = date_utils.now() + datetime.timedelta(res_cache_analyticengine_validity)
        elif 'SnapshotSegmentSnapshotTask' in v2task.path or \
            'snapshot_task' in v2task.path or \
                'DealChangesTotalsTask' in v2task.path or \
            'deal_changes_totals' in v2task.path or \
            'SnapshotAccountsTask' in v2task.path or \
            'snapshot_accounts_task' in v2task.path or \
            'FilterTotalsMainTask' in v2task.path or \
            'filter_totals_task' in v2task.path:
            shell.expires = date_utils.now() + datetime.timedelta(adhoc_task_validity)
        shell.save()
        return shell

    def mark_as_failed(self, failed_tasks):
        self.failed_tasks = failed_tasks
        self.status = self.FAILED
        self.save(is_partial=True, field_list=['object.status',
                                               'object.failed_tasks'])

    def save(self, is_partial=False, bulk_list=None, id_field=None, field_list=None):
        if self.status == 2:
            ResultCacheArchive.archive_data(self)
        return super(ResultCache, self).save(is_partial, bulk_list, id_field, field_list)


class ResultCacheArchive(ResultCache):

    collection_name = 'resultcache_archive'
    tenant_aware = True
    encrypted = False
    read_from_primary=True

    def __init__(self, attrs=None):
        super(ResultCacheArchive, self).__init__(attrs)

    def encode(self, attrs):
        return super(ResultCacheArchive, self).encode(attrs)

    def decode(self, attrs):
        return super(ResultCacheArchive, self).decode(attrs)

    @classmethod
    def archive_data(cls, result_obj):
        # no need to archive result_cache records in analytics-server.
        return
        try:
            logger.info("Archiving result cache for resid %s" % (result_obj.extid))
            res_cache_data = {}
            result_to_archive = ResultCacheArchive()
            res_cache_data = result_obj.__dict__
            # copy data from task to archive task
            for i in res_cache_data:
                setattr(result_to_archive, i, res_cache_data[i])
            if 'analyticengine' in result_obj.path:
                result_to_archive.expires = dateUtils.now() + datetime.timedelta(res_archive_analyticengine_validity)
            elif 'SnapshotSegmentSnapshotTask' in result_obj.path or \
                    'snapshot_task' in result_obj.path or \
                    'DealChangesTotalsTask' in result_obj.path or \
                    'deal_changes_totals' in result_obj.path or \
                    'SnapshotAccountsTask' in result_obj.path or \
                    'snapshot_accounts_task' in result_obj.path or \
                    'FilterTotalsMainTask' in result_obj.path or \
                    'filter_totals_task' in result_obj.path:
                result_to_archive.expires = dateUtils.now() + datetime.timedelta(adhoc_task_validity)
            else:
                result_to_archive.expire()
            result_to_archive.save()
        except Exception:
            logger.info("Unable to archive result for res_id %s due to exception %s" %
                        (res_cache_data.get("extid"), traceback.format_exc()))

    def expire(self):
        self.expires = date_utils.now() + datetime.timedelta(res_archive_validity)

    def save(self, is_partial=False, bulk_list=None, id_field=None, field_list=None):
        return Model.save(self, is_partial, bulk_list, id_field, field_list)


class ResultCache(Model):
    collection_name = 'result_cache'
    tenant_aware = True
    encrypted = False
    read_from_primary=True
    WAITING = 0
    FAILED = 1
    AVAILABLE = 2

    kind = 'domainmodel.app.ResultCache'
    version = 1
    index_list = {
        'expires': {
            'expireAfterSeconds': 7200
        },
        'extid': {},
        'status': {},
        'requesting_task': {},
        'cache_key': {},
        'as_of_mnemonic': {},
        'path': {},
    }

    def __init__(self, attrs=None):
        self.extid = None
        self.expire()
        #  Available, Failed, Waiting
        self.status = None
        self.lifetime = 'short'
        self.requesting_task = None
        self.failed_tasks = []
        self.path = None
        self.cache_key = None
        self.as_of_mnemonic = None
        self.deps = []
        self.filesize = {'compressed': 0,
                         'uncompressed': 0}
        self.result = {}
        self.cached = True

        super(ResultCache, self).__init__(attrs)

    def encode(self, attrs):
        attrs['extid'] = self.extid
        attrs["expires"] = self.expires
        attrs['lifetime'] = self.lifetime
        attrs['status'] = self.status
        attrs['requesting_task'] = self.requesting_task
        attrs['failed_tasks'] = self.failed_tasks
        attrs['path'] = self.path
        attrs['cache_key'] = self.cache_key
        attrs['as_of_mnemonic'] = self.as_of_mnemonic
        attrs['deps'] = self.deps
        attrs['filesize'] = self.filesize
        attrs['result'] = self.result
        attrs['cached'] = self.cached

    def decode(self, attrs):
        self.extid = attrs['extid']
        self.lifetime = attrs.get('lifetime', 'short')
        if 'expires' in attrs:
            self.expires = attrs["expires"]
        else:
            self.expires = None
        self.status = attrs.get('status', 'Created')
        self.requesting_task = attrs.get('requesting_task')
        self.failed_tasks = attrs.get('failed_tasks')
        self.path = attrs.get('path')
        self.cache_key = attrs.get('cache_key', None)
        self.as_of_mnemonic = attrs.get('as_of_mnemonic')
        self.deps = attrs.get('deps')
        self.filesize = attrs.get('filesize', {'compressed': '0', 'uncompressed': 0})
        self.result = attrs.get('result', {})
        self.cached = attrs.get('cached', True)

    @classmethod
    def getByKey(cls, extid):
        return cls.getByFieldValue('extid', extid)

    def expire(self):
        self.expires = datetime.datetime.now(UTC) + datetime.timedelta(res_cache_validity)

    def extend_expire(self):
        # extend the TTL of this cache result if it's used as a result tree.
        # only extend it if the cache life of this object is already 1/2 over however

        expires_in = (self.expires - datetime.datetime.now(UTC)).total_seconds()
        fuzzy_window = 300  #  five 5 mins so it does not update continuously while creating a new tree.
        half_of_cache_time = datetime.timedelta(res_cache_validity).total_seconds()/2

        if (expires_in + fuzzy_window) < half_of_cache_time:
            self.expire()
            self.save(is_partial=True, field_list=["object.expires"])

    @classmethod
    def add_result(cls, v2task):
        shell = cls()
        shell.lifetime = v2task.lifetime
        shell.status = cls.WAITING
        shell.extid = v2task.res_id
        shell.requesting_task = v2task.task_id
        shell.path = v2task.path
        shell.cache_key = v2task.cache_key
        shell.as_of_mnemonic = v2task.as_of_mnemonic
        shell.deps = list(set(v.res_id for k, v in v2task.dependencies.items()))
        shell.cached = v2task.is_cached()
        if 'analyticengine' in v2task.path:
            shell.expires = date_utils.now() + datetime.timedelta(res_cache_analyticengine_validity)
        elif 'SnapshotSegmentSnapshotTask' in v2task.path or \
            'snapshot_task' in v2task.path or \
                'DealChangesTotalsTask' in v2task.path or \
            'deal_changes_totals' in v2task.path or \
            'SnapshotAccountsTask' in v2task.path or \
            'snapshot_accounts_task' in v2task.path or \
            'FilterTotalsMainTask' in v2task.path or \
            'filter_totals_task' in v2task.path:
            shell.expires = date_utils.now() + datetime.timedelta(adhoc_task_validity)
        shell.save()
        return shell

    def mark_as_failed(self, failed_tasks):
        self.failed_tasks = failed_tasks
        self.status = self.FAILED
        self.save(is_partial=True, field_list=['object.status',
                                               'object.failed_tasks'])

    def save(self, is_partial=False, bulk_list=None, id_field=None, field_list=None):
        if self.status == 2:
            ResultCacheArchive.archive_data(self)
        return super(ResultCache, self).save(is_partial, bulk_list, id_field, field_list)


class Task(Model):
    """Stores information about a task"""
    CSV_RESULT = 1
    encrypted = False
    JSON_RESULT = 2
    BINARY_RESULT = 3
    mime_types = {CSV_RESULT: "application/csv",
                  JSON_RESULT: "application/json",
                  BINARY_RESULT: "application/binary"}
    collection_name = 'task'
    tenant_aware = False
    kind = 'domainmodel.task.Task'
    version = 1
    index_list = {
        'expires': {
            'expireAfterSeconds': 7200
        },
        'extid': {},
        'main_id': {},
        'res_id': {},
        'consumers': {},
        'createdby': {},
        'tasktype': {},
        'created_datetime': {},
        'trace': {},
        'tenant': {},
        'submit_time': {},
        'task_meta.request_params.params': {'sparse': True},
        'celery_id': {},
        'status': {},
        'ip_address': {},
        'framework_version': {},
        'cname': {}
    }
    STATUS_CREATED = 0
    STATUS_STARTED = 1
    STATUS_FINISHED = 2
    STATUS_ERROR = 3
    STATUS_SUBMITTED = 4
    STATUS_TERMINATED = 5
    STATUS_REVOKED = 6
    # Remember to keep - TASK_STATUS_STRS above up to date.

    # it is assumed that task related collection in preprod will go to primary db.
    read_from_primary=True

    def __init__(self, attrs=None):
        self.type = None
        self.createdby = None  # Should be the ObjectID
        self.progress = -1
        self.steps = []
        self.current_step = None
        self.extid = None
        self.status = None
        self.current_status_msg = ""
        # Stored as epoch, so we can sort on it easily.
        self.created_datetime = date_utils.now()  # datetime2epoch(datetime.datetime.now())
        self.task_meta = {}
        self.trace = None
        self.main_id = None
        self.res_id = None
        self.celery_id = None
        self.consumers = []
        self.perc = 0.0
        self.tenant = None
        self.submit_time = None
        self.start_time = None  # when task started
        self.stop_time = None  # when task stopped (completed of failed)
        self.did_work = None   # did it have to do processing or was result already cached
        self.framework_version = "1"  # make sure to use string. new framework will be "2"
        self.build_id = None
        self.deps = {}
        self.ip_address = None
        self.pid = None
        self.mem_required = None
        self.pool_name = None
        self.v2_run_type = None
        self.cname = POOL_PREFIX
        self.mem_consumed = None
        self.heartbeat = None
        self.retry_count = None
        self.retry_strategy = None
        self.retry_expires = None
        self.expire()
        self.pinned = True
        self.cached = True
        self.level = 1
        self.ready_since = None
        super(Task, self).__init__(attrs)

    @staticmethod
    def register_new_task(v2task, parent_id, main_id=None, save=True, build_id=None):
        user, tenant, domain, login_user_name, switch_type, csv_version_info = sec_context.peek_context()
        t = Task()
        res_id = v2task.res_id
        t.framework_version = "2"
        t.build_id = build_id
        t.status = Task.STATUS_CREATED
        t.type = v2task.path
        t.cached = v2task.is_cached()
        t.pinned = v2task.is_pinned()
        t.level = v2task.level
        t.retry_strategy = v2task.retry_strategy or t.retry_strategy
        t.retry_expires = v2task.retry_expires or t.retry_expires

        # need to fix domain in analyticengine-config.py files, as domain = tenant_name
        domain = 'administrative.domain'
        t.createdby = '{}@{}'.format(user, domain)
        t.trace = tracer.trace
        if main_id:
            t.main_id = main_id
        else:
            t.main_id = t.get_mainid(tracer.trace)
        t.res_id = res_id
        t.extid = v2task.task_id
        t.celery_id = v2task.task_id

        t.task_meta = {'params': v2task.params,
                       'context': v2task.context,
                       'lifetime': v2task.lifetime}
        t.tenant = tenant
        t.consumers = v2task.consumers
        if parent_id not in t.consumers and parent_id != v2task.task_id:
            t.consumers.append(parent_id)
        t.deps = dict((k, v.res_id) for k, v in v2task.dependencies.items())
        logger.info("*** - registering %s, %s, %s, build_id: %s",
                    v2task.name, res_id, v2task.task_id, build_id)
        pool_name = v2task.params.get('pool_name', None) or v2task.context.get('pool_name', None)
        t.pool_name = pool_name if pool_name is not None else WORKER_POOL
        t.cname = POOL_PREFIX
        if save:
            t.save()
        else:
            return t

    @staticmethod
    def register_bulk_tasks(tasks):
        batch = 1000
        for i in range(0, len(tasks['task'].values()), batch):
            bulk_tasks = tasks['task'].values()[i:i + batch]
            bulk_activetasks = tasks['activetask'].values()[i:i + batch]
            Task.bulk_insert(bulk_tasks)
            TaskActive.bulk_insert(bulk_activetasks)

    def mark_as_submitted(self, mem, node, run_type):

        if self.status == Task.STATUS_ERROR:
            criteria = {'object.extid': self.res_id, 'object.requesting_task': self.extid}
            res = ResultCache.getBySpecifiedCriteria(criteria)
            if res:
                res.status = ResultCache.FAILED
                res.save(is_partial=True, field_list=['object.status'])
            TaskActive.truncate_or_drop({'object.extid': self.extid})
            logger.exception(f"on_submit found error status for task {self.extid}. So, revoking this task")
            raise Exception("on_submit found error status")
        self.submit_time = date_utils.now()
        self.ip_address = node
        self.mem_required = mem
        self.v2_run_type = run_type
        self.status = Task.STATUS_SUBMITTED
        if 'SnapshotSegmentSnapshotTask' in self.type or \
                 'snapshot_task' in self.type or \
                'DealChangesTotalsTask' in self.type or \
                'deal_changes_totals' in self.type or \
                'SnapshotAccountsTask' in self.type or \
                'snapshot_accounts_task' in self.type or \
                'FilterTotalsMainTask' in self.type or \
                'filter_totals_task' in self.type:
            self.expires = date_utils.now() + datetime.timedelta(adhoc_task_validity)
        self.save(is_partial=True, field_list=['object.status',
                                               'object.submit_time',
                                               'object.ip_address',
                                               'object.mem_required',
                                               'object.v2_run_type'])

    def add_consumer(self, parent_id):
        self.consumers.append(parent_id)
        self.save()

    def encode(self, attrs):
        attrs['extid'] = self.extid
        if not self.type or not self.createdby:
            raise TaskError("Task type and created by or mandatory")
        attrs['tasktype'] = self.type
        attrs['createdby'] = self.createdby
        attrs['status'] = self.status
        attrs["expires"] = self.expires
        attrs['created_datetime'] = self.created_datetime
        attrs['current_status_msg'] = self.current_status_msg
        attrs['consumers'] = self.consumers
        attrs['perc'] = self.perc
        attrs['did_work'] = self.did_work
        attrs['main_id'] = self.main_id
        attrs['res_id'] = self.res_id
        attrs['celery_id'] = self.celery_id
        attrs['task_meta'] = self.task_meta
        attrs['tenant'] = self.tenant
        attrs['trace'] = self.trace
        attrs['submit_time'] = self.submit_time
        attrs['start_time'] = self.start_time
        attrs['stop_time'] = self.stop_time
        attrs['framework_version'] = str(self.framework_version)
        attrs['build_id'] = self.build_id
        attrs['deps'] = self.deps
        attrs['ip_address'] = self.ip_address
        attrs['pid'] = self.pid
        attrs['mem_required'] = self.mem_required
        attrs['pool_name'] = self.pool_name
        attrs['v2_run_type'] = self.v2_run_type
        attrs['cname'] = self.cname
        attrs['mem_consumed'] = self.mem_consumed
        attrs['heartbeat'] = self.heartbeat
        attrs['retry_count'] = self.retry_count
        attrs['retry_strategy'] = self.retry_strategy
        attrs['retry_expires'] = self.retry_expires
        attrs['pinned'] = self.pinned
        attrs['cached'] = self.cached
        attrs['level'] = self.level
        attrs['ready_since'] = self.ready_since
        return super(Task, self).encode(attrs)

    def decode(self, attrs):
        self.type = attrs.get('tasktype', None)
        self.createdby = attrs.get("createdby", None)
        self.extid = attrs.get("extid", None)
        self.consumers = attrs.get('consumers', []) or attrs.get("parent_id", [])
        self.perc = attrs.get('perc', None)
        self.main_id = attrs.get('main_id', None)
        self.res_id = attrs.get('res_id', None)
        self.celery_id = attrs.get('celery_id', None)
        self.status = attrs.get("status", None)
        self.trace = attrs.get('trace', None)
        self.expires = attrs.get("expires", None)
        self.tenant = attrs.get('tenant', None)
        self.did_work = attrs.get('did_work', None)
        self.created_datetime = attrs.get('created_datetime', None)
        self.task_meta = attrs.get('task_meta', None)
        self.current_status_msg = attrs.get('current_status_msg', None)
        self.submit_time = attrs.get('submit_time', None)
        self.start_time = attrs.get('start_time', None)
        self.stop_time = attrs.get('stop_time', None)
        self.framework_version = attrs.get('framework_version', "1")
        self.build_id = attrs.get('build_id', None)
        self.deps = attrs.get('deps', {})
        self.ip_address = attrs.get('ip_address', None)
        self.pid = attrs.get('pid', None)
        self.mem_required = attrs.get('mem_required', None)
        self.pool_name = attrs.get('pool_name', None)
        self.v2_run_type = attrs.get('v2_run_type', None)
        self.cname = attrs.get('cname', POOL_PREFIX)
        self.mem_consumed = attrs.get('mem_consumed', None)
        self.heartbeat = attrs.get('heartbeat', None)
        self.retry_count = attrs.get('retry_count', None)
        self.retry_strategy = attrs.get('retry_strategy', None)
        self.retry_expires = attrs.get('retry_expires', None)
        self.pinned = attrs.get('pinned', True)
        self.cached = attrs.get('cached', True)
        self.level = attrs.get('level', 1)
        self.ready_since = attrs.get('ready_since', None)
        return super(Task, self).decode(attrs)

    def expire(self):
        self.expires = date_utils.now() + datetime.timedelta(task_validity)

    @classmethod
    def updateCurrentStatus(cls, status_as_string, task_id=None):
        """ For a running task, updates a running comment on where we are.
        The 'current_status_msg' field is updated with whatever has been
        provided. Tasks such as dataload, answers, etc. can update what
        they are doing at various stages, so when the task status is queried
        we can get a hint of where we are. This is especially useful when
        running models for large datasets. For example, answers.py can
        update the task status as, "Processing records 500-600 of 25234", etc.
        The API has been added here, so other tasks can simply call it on Task.
        """
        task_id = task_id if task_id else current_task.request.id
        t = cls.getByFieldValue('extid', task_id)
        if t:
            try:
                t.current_status_msg = status_as_string
                t.save(is_partial=True, field_list=['object.current_status_msg'])
            except:
                # We are not going to stop the world because of this failure.
                pass

    @classmethod
    def try_set_ready_since(cls, task=None, ready_since=None):

        if isinstance(task, str):
            t = cls.getByFieldValue('extid', task)
        else:
            t = task

        if t:
            try:
                t.ready_since = ready_since or datetime.datetime.now(UTC)
                t.save(is_partial=True, field_list=['object.ready_since'])
                return True
            except:
                # We are not going to stop the world because of this failure.
                return False

    @classmethod
    def getATask(cls, task_id=None):
        task_id = task_id if task_id else current_task.request.id
        return cls.getByFieldValue('extid', task_id)

    @classmethod
    def get_mainid(cls, trid):
        p = cls.getByFieldValue('trace', trid)
        return p.main_id if p else None

    @classmethod
    def set_task(cls, trid, mid, tid, pid, stat, di, na):
        p = cls.getByFieldValue('extid', tid)
        if not p:
            p = Task()
            q = cls.getByFieldValue('trace', trid)
            p.trace = trid
            p.main_id = mid
            p.extid = tid
            p.consumers = pid
            if di:
                p.task_meta = di
            p.type = na
            p.createdby = q.createdby
            p.status = stat
            p.tenant = sec_context.name
            p.celery_id = tid
            p.pool_name = WORKER_POOL
            p.cname = POOL_PREFIX
        else:
            if not p.celery_id:
                p.celery_id = tid
        return p.save()

    def mark_as_failed(self):
        self.status = self.STATUS_ERROR
        self.save(is_partial=True, field_list=['object.status'])

    def retry_expire_time(self):
        if self.retry_expires:
            return self.retry_expires
        if self.task_meta.get('context', {}).get('retry_expires', None):
            return self.task_meta.get('context', {}).get('retry_expires', None)
        return self.created_datetime + datetime.timedelta(seconds=task_expire_seconds)

    def status_str(self):
        if self.status is None:
            return "NONE"
        if self.status in TASK_STATUS_STRS:
            return TASK_STATUS_STRS[self.status]
        return "UNKNOWN"


class TaskArchive(Task):

    tenant_aware = True
    encrypted = False
    read_from_primary=True

    def __init__(self, attrs=None):
        self.tags = None
        super(TaskArchive, self).__init__(attrs)

    def encode(self, attrs):
        attrs["tags"] = self.tags
        return super(TaskArchive, self).encode(attrs)

    def decode(self, attrs):
        self.tags = attrs.get("tags", None)
        return super(TaskArchive, self).decode(attrs)

    @classmethod
    def archive_data(cls, task_obj):
        # no need to archive task records in analytics-server.
        return

        from tasks import gnana_task_support
        try:
            logger.info(f"Archiving task {task_obj.extid}")
            task_data = {}
            task_to_archive = TaskArchive()
            task_data = task_obj.__dict__
            if "expires" in task_data:
                del task_data["expires"]
            if 'analyticengine' in task_obj.type:
                task_data["expires"] = dateUtils.now() + datetime.timedelta(archive_analyticengine_validity)
            elif 'SnapshotSegmentSnapshotTask' in task_obj.path or \
                 'snapshot_task' in task_obj.path or \
                'DealChangesTotalsTask' in task_obj.path or \
                'deal_changes_totals' in task_obj.path or \
                'SnapshotAccountsTask' in task_obj.path or \
                'snapshot_accounts_task' in task_obj.path or \
                'FilterTotalsMainTask' in task_obj.path or \
                'filter_totals_task' in task_obj.path:
                task_data['expires'] = dateUtils.now() + datetime.timedelta(adhoc_task_validity)
            # copy data from task to archive task
            for i in task_data:
                setattr(task_to_archive, i, task_data[i])
            task_to_archive.tags = getattr(gnana_task_support.thread_local_tags, "tags", None)
            task_to_archive.save()
        except Exception:
            logger.info("Unable to archive task %s due to exception %s" %
                        (task_data.get("extid"), traceback.format_exc()))

    def expire(self):
        self.expires = date_utils.now() + datetime.timedelta(archive_task_validity)


class V2StatsLog(Model):
    collection_name = 'v2statslog'

    encrypted = False
    tenant_aware = True
    postgres = False
    read_from_primary=True

    kind = 'domainmodel.app.V2StatsLog'
    version = 1
    index_list = {
        'path': {},
    }

    def __init__(self, attrs=None):
        self.path = None
        self.memory = None
        self.cpu = None
        self.time_taken = None
        self.run_type = None
        super(V2StatsLog, self).__init__(attrs)

    def encode(self, attrs):
        attrs['path'] = self.path
        attrs['memory'] = self.memory
        attrs['cpu'] = self.cpu
        attrs['time_taken'] = self.time_taken
        attrs['run_type'] = self.run_type

    def decode(self, attrs):
        self.path = attrs['path']
        self.memory = attrs['memory']
        self.cpu = attrs['cpu']
        self.time_taken = attrs['time_taken']
        self.run_type = attrs['run_type']

    @classmethod
    def getByKey(cls, path):
        return cls.getByFieldValue('path', path)

    @classmethod
    def addv2statslog(cls, path, mem_used, cpu_perc, time_taken, run_type):
        stats = cls()
        stats.path = path
        stats.memory = mem_used
        stats.cpu = cpu_perc
        stats.time_taken = time_taken
        stats.run_type = run_type
        stats.save()
