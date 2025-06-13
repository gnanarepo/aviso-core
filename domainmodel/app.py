import datetime
import json
import logging
import re
import time
from datetime import timezone

UTC = timezone.utc

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



class TaskError(Exception):

    def __init__(self, error):
        self.error = error





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
    def addv2statslog(cls, path, mem_used, cpu_perc, time_taken, run_type):
        stats = cls()
        stats.path = path
        stats.memory = mem_used
        stats.cpu = cpu_perc
        stats.time_taken = time_taken
        stats.run_type = run_type
        stats.save()
