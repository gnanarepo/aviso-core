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
from ..domainmodel.model import Model
from ..utils import date_utils

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
