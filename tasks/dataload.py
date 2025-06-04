import datetime
import logging

import celery
from aviso.settings import gnana_db, sec_context
from aviso.framework import tracer
import pytz

from domainmodel.datameta import Dataset
from domainmodel.uip import MAX_CREATED_DATE
from tasks import asynctasks, gnana_task_support
from utils import queue_name


logger = logging.getLogger('gnana.%s' % __name__)


@celery.task
def process_inbox_entries(user_and_tenant, dsname, inboxname, idlist, **kwargs):
    return process_inbox_entries_task(user_and_tenant, dsname, inboxname, idlist, **kwargs)


@gnana_task_support
def process_inbox_entries_task(user_and_tenant, dsname, inboxname, idlist, **kwargs):
    ds = Dataset.getByNameAndStage(dsname, inboxname)
    for x in idlist:
        if not x:
            continue
        record = ds.DatasetClass.getByFieldValue('extid', x)
        if not record:
            record = ds.DatasetClass(extid=x)
        docs = gnana_db.findDocuments(ds.InboxEntryClass.getCollectionName(),
                                      {'object.extid': sec_context.encrypt(x)})
        for doc in docs:
            if doc['_kind'] == 'domainmodel.uip.InboxFileEntry':
                logger.exception("Unexpected Kind of object")
            inbox_entry = ds.InboxEntryClass(doc)
            file_type = ds.filetype(inbox_entry.filetype)
            file_type.apply_inbox_record(record, inbox_entry, fld_config=ds.fields)
        record.compress()
        if not record.created_date == MAX_CREATED_DATE:
            record.save()
        else:
            logger.warning(
                'Ignoring record with id %s has no fields.', record.ID)


def merge_inbox_entries_to_uip(user_and_tenant, datasetname, stagename,
                               auto_purge=True):
    dataset = Dataset.getByNameAndStage(datasetname, stagename, full_config=False)

    # Get the unique ext-id values and pass 1000
    # at a time to workers for processing
    distinct_entries = gnana_db.getDistinctValues(
        dataset.InboxEntryClass.getCollectionName(),
        "object.extid")
    record_count = len(distinct_entries)

    tasks = []
    while (distinct_entries):
        firstslice = distinct_entries[0:1000]
        distinct_entries = distinct_entries[1000:]
        tasks.append(process_inbox_entries.subtask(args=(user_and_tenant, dataset.name, stagename, firstslice,),
                                                   kwargs={
                                                       'trace': tracer.trace},
                                                   options={'queue': queue_name('worker')}))

    if (tasks):
        async_res = [asynctasks.subtask_handler(
            t, queue=queue_name('worker'), d=2) for t in tasks]
        # We have to wait for the response.  Otherwise we will drop the
        # inboxes while the id's are in process
        for t in async_res:
            t.get()

    docs = gnana_db.findDocuments(dataset.InboxEntryClass.getCollectionName(),
                                  {u"_kind": u"domainmodel.uip.InboxFileEntry"})
    types_and_dates = {}
    for file_entry in docs:
        f = dataset.InboxFileEntryClass(file_entry)
        if f.filetype in types_and_dates:
            if f.when:
                types_and_dates[f.filetype] = max(
                    types_and_dates[f.filetype], f.when)
        else:
            types_and_dates[f.filetype] = f.when or datetime.datetime(
                1970, 1, 1, tzinfo=pytz.utc)

    for each_type, value_for_type in types_and_dates.items():
        dataset.file_types[each_type].last_approved_time = value_for_type

    dataset.save()

    if auto_purge:
        # Clear the inbox, we don't need it anymore
        gnana_db.dropCollection(dataset.InboxEntryClass.getCollectionName())

    return record_count
