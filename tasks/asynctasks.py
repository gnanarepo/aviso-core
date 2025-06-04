import datetime
import logging

from aviso.framework import tracer
from aviso.framework.diagnostics import probe_util
from aviso.settings import POOL_PREFIX, WORKER_POOL, event_context
from celery import current_task

from domainmodel.app import Task

logger = logging.getLogger('gnana.%s' % __name__)


def subtask_handler(t, kwargs=None, queue=None, args=(), d=0):
    if kwargs is None:
        kwargs = {}
    tid = None
    '''
    Adding the event id to kwargs to pass it onto other child tasks for tenant events.
    The event id stored in thread local as part of event context.
    '''
    te_id = event_context.event_id
    if d == 0:
        kwargs['event_id'] = te_id
        tid = t.apply_async(args=args, kwargs=kwargs, queue=queue)

    elif d == 1:
        t.kwargs['event_id'] = te_id
        tid = t.apply_async()

    elif d == 2:
        t.kwargs['event_id'] = te_id
        tid = t.apply_async(queue=queue)

    if not current_task:
        ts = Task.getByFieldValue('trace', tracer.trace)
        ts.extid = tid.id
        ts.celery_id = tid.id
        ts.main_id = tid.id
        ts.submit_time = datetime.datetime.utcnow()
        ts.status = Task.STATUS_SUBMITTED
        ts.pool_name = WORKER_POOL
        ts.cname = POOL_PREFIX
        ts.save(is_partial=True, field_list=['object.extid',
                                             'object.celery_id',
                                             'object.main_id',
                                             'object.submit_time',
                                             'object.status',
                                             'object.pool_name',
                                             'object.cname'])
    else:
        pid = current_task.request.id
        na = tid.task_name
        di = {}
        if kwargs:
            di['kwargs'] = kwargs
        if args:
            di['args'] = args

        if not Task.get_mainid(tracer.trace):
            logger.info("Main task is None for (%s)", current_task.request.id)
        Task.set_task(tracer.trace, Task.get_mainid(tracer.trace), tid.id, pid, Task.STATUS_SUBMITTED, di, na)

    return tid
