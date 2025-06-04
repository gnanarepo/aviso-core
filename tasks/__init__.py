import json
import logging
import os
import shutil
import sys
import tempfile
import threading
import time
import traceback
import uuid
from datetime import datetime

import celery
from _collections import defaultdict
from aviso.framework import tracer
from aviso.framework.diagnostics import probe_util
from aviso.framework.views import GnanaError
from aviso.settings import (CNAME, DEBUG, TMP_DIR, event_context,
                            gnana_cprofile_bucket, gnana_db, gnana_storage,
                            log_config, node, sec_context)
from celery import current_task
from celery.result import AsyncResult

from domainmodel.app import (ResultCache, Task, TaskActive, TaskArchive,
                             V2StatsLog)
from tasks import asynctasks
from utils import date_utils, file_utils, memory_usage_resource

logger = logging.getLogger('gnana.%s' % __name__)
task_statuses = {
    0: 'Created',
    1: 'Started',
    2: 'Finished',
    3: 'Error',
    4: 'Submitted',
    5: 'Terminated'
}
large_response_key = 'large_response_key'
dup_celery_id = 'duplicate_celery_id'
filename_key = 'filename'

thread_local_tags = threading.local()

def celery_task_handler(tasks, cleanup, kill_siblings_on_fail=False, d=0):
    def find_kill_children(traceid):
        criteria = {'object.consumers': traceid}
        result = Task.getAll(criteria)
        for t in result:
            res = AsyncResult(t.extid)
            if(res.state not in ['SUCCESS', 'FAILURE']):
                t.status = Task.STATUS_ERROR
                t.save(is_partial=True, field_list=['object.status'])
                t.current_status_msg = 'terminated'
                res.revoke(terminate=True, signal='SIGKILL')
    results = []
    failed_task_message = ''
    failed = False
    if d == 0:
        task_list = [asynctasks.subtask_handler(
            t[0], kwargs=t[1], queue=t[2]) for t in tasks]
    elif d == 1:
        task_list = [asynctasks.subtask_handler(t=t, d=d) for t in tasks]
    elif d == 2:
        task_list = [
            asynctasks.subtask_handler(t=t[0], queue=t[1], d=d) for t in tasks]
    dup = []
    copy_list = []
    for task_entry in task_list:
        if task_entry in copy_list:
            dup.append(task_entry)
        else:
            copy_list.append(task_entry)
    if dup:
        failed = True
        logger.error("Duplicate task ids in task list %s" % dup)
    while task_list and not dup:
        remainingtasks = task_list
        time.sleep(2)
        try:
            for t in remainingtasks:
                if t.ready():
                    # if isinstance(result,list) else results.append(result)
                    resp = t.get()
                    if isinstance(resp, dict):
                        if large_response_key in resp:
                            resp = load_response(resp)
                        elif dup_celery_id in resp:
                            raise Exception("Tasks are having duplicate celery_id")
                    results.append(resp)
                    task_list.remove(t)

        except Exception as ex:
            failed = True
            failed_task_message += t.id + \
                ' failed with the message' + ex.message
            task_list.remove(t)
            if kill_siblings_on_fail:
                break

    if kill_siblings_on_fail and failed:
        for t in task_list:
            mytask = Task.getByFieldValue('extid', t.task_id)
            mytask.status = Task.STATUS_ERROR
            mytask.current_status_msg = 'terminated' + failed_task_message
            mytask.save(is_partial=True, field_list=['object.status',
                                                     'object.current_status_msg'])
            t.revoke(terminate=True, signal='SIGKILL')
            find_kill_children(t.task_id)
    if failed:
        if cleanup is not None:
            cleanup()
        raise Exception(
            'Failures detected in subtasks:\n ' + failed_task_message)
    return results


class CacheDBInterface:
    def get_cache(self, res_id):
        return ResultCache.getByFieldValue('extid', res_id)

    def getBySpecifiedCriteria(self, criteria):
        return ResultCache.getBySpecifiedCriteria(criteria)

    def save_cache(self, res, **kwargs):
        if 'is_partial' not in kwargs:
            kwargs['is_partial'] = True
        res.save(**kwargs)


class TaskDBInterface:
    def fetch_task(self, task_id):
        t = Task.getByFieldValue('celery_id', task_id)
        if not t:
            t = Task.getByFieldValue('extid', task_id)
        return t

    def fetch_celery_task(self, task_id):
        return Task.getByFieldValue('celery_id', task_id) if task_id else None

    def save_task(self, task, **kwargs):
        if 'is_partial' not in kwargs:
            kwargs['is_partial'] = True
        task.save(**kwargs)

    def archive(self, task):
        TaskArchive.archive_data(task)

    def heartbeat(self, task):
        task.heartbeat = date_utils.now()
        self.save_task(task,  field_list=['object.heartbeat'])


def gnana_task_support(f):
    # type: (object) -> object
    """ A decorator function that wraps the given function with the ability
    to set the tenant name in tenant local, and saves the return values
    for later retrieval """
    fn = f

    def heartbeat_main(task, task_db, heartbeat_stop_event):
        #sec_context.set_context('gbm', task.tenant, 'administrative.domain', 'gbm', 'tenant', {})
        tracer.trace = task.trace
        start_time = datetime.utcnow()
        last_time = start_time

        while task.status == Task.STATUS_STARTED and not heartbeat_stop_event.is_set():
            heartbeat_stop_event.wait(timeout=10.0)
            try:
                if not heartbeat_stop_event.is_set():
                    task_db.heartbeat(task)
            except Exception:
                logger.warn("Unable to save task heartbeat.", exc_info=True)

            hb_metrics = logger.new_metrics()
            hb_metrics.set_timer("task.elapsed_time", (datetime.utcnow() - start_time).total_seconds() * 1000)
            hb_metrics.set_timer("task.heartbeat_time", (datetime.utcnow() - last_time).total_seconds() * 1000)
            logger.metrics("Task Heartbeat " + task.extid, metrics=hb_metrics)
            last_time = datetime.utcnow()

    def new_task(user_and_tenant, *args, **options):
        thread_local_tags.tags = {}
        user_name = user_and_tenant[0]
        tenantname = user_and_tenant[1]
        logintenant = user_and_tenant[2]

        cache_db = CacheDBInterface() if 'cache_db' not in options else options['cache_db']
        task_db = TaskDBInterface() if 'task_db' not in options else options['task_db']
        task_metrics = logger.new_metrics()
        task_metrics.new_timer('task.time')

        cprofile_filename = ''
        try:
            csv_version_info = user_and_tenant[3]
        except:
            csv_version_info = {}

        # Set the tenant name first
        try:
            oldname = (
                sec_context.user_name, sec_context.name, sec_context.login_tenant_name, sec_context.csv_version_info)
        except AttributeError:
            oldname = None

        # Setup logging
        if log_config:
            log_config.restart()

        if 'trace' in options:
            old_trace = tracer.trace
            tracer.trace = options['trace']
        else:
            old_trace = None
            # Make sure a new trace id is generated
            tracer.trace = None

        t = None
        failed = False
        import psutil
        pid = os.getpid()
        process = psutil.Process(pid)
        start_memory = process.memory_full_info().rss / (1024 * 1024)
        start_time = time.time()
        starting_cpu_times = process.cpu_times()
        heartbeat_stop_event = None
        try:
            # Set the tenant name
            sec_context.set_context(user_name, tenantname, logintenant, csv_version_info=csv_version_info, override_version_info=True)
            event_id = options.get('event_id', None)
            if event_id:
                event_context.set_event_context(event_id)
            name = None
            if options.get('v2task_id', None):
                celery_id = options.get('v2task_id', None)
            else:
                celery_id = current_task.request.id
                name = current_task.name
            t = task_db.fetch_celery_task(celery_id)
            if name is None:
                name = t.type
            logger.info(
                "Begin execution of Task %s[%s]", name, celery_id)
            if(t):
                logger.set_common_tags({'task.id': t.extid, 'task.name': t.type})

                if t.tenant != tenantname:
                    logger.error("Task tenant name does not match %s != %s", t.tenant, tenantname)
                    raise Exception("Task tenant name does not match %s != %s", t.tenant, tenantname)

                for i in range(3):
                    if t.status != 4:
                        time.sleep(i+1)
                        t = task_db.fetch_celery_task(celery_id)
                    else:
                        break
                # avoid created status
                add_ip_address = False
                if t.framework_version == '2' and t.status == 0:
                    if t.ip_address != node:
                        t.ip_address = node
                        add_ip_address = True
                    logger.info("Task %s is submitted but task status is still showing %s,\
                     so starting task assuming there could be db latency" % (t.extid,
                                                                             task_statuses[t.status]))
                elif t.status != Task.STATUS_SUBMITTED:  # 4 stands for Submitted
                    incorrect_state = [1, 2]
                    if t.framework_version == '1':
                        incorrect_state = [0, 1, 2]
                    if t.status in incorrect_state:
                        logger.warn("In-correct state, not expecting %s ", task_statuses[t.status])
                        return {'duplicate_celery_id': []}
                    logger.info("Task %s[%s] has been terminated",
                                name, celery_id)
                    logger.info("Expected status Submitted but has status %s",
                                task_statuses[t.status])
                    if t.status != Task.STATUS_TERMINATED:
                        t.stop_time = date_utils.now()
                        t.current_status_msg = 'Task Error'
                        t.status = Task.STATUS_ERROR
                        memory_use = memory_usage_resource()
                        t.mem_consumed = memory_use
                        task_db.save_task(t, field_list=['object.status', 'object.current_status_msg',
                                                         'object.stop_time', 'object.mem_consumed'])
                    TaskArchive.archive_data(t)
                    failed = True
                    return
                t.pid = pid
                t.status = Task.STATUS_STARTED
                t.start_time = date_utils.now()
                fld_list = ['object.status', 'object.pid', 'object.start_time']
                if add_ip_address:
                    fld_list.append('object.ip_address')
                task_db.save_task(t, field_list=fld_list)

            # Check for probeid and do the needful
            if 'probeid' in options:
                location = "TASK"
                try:
                    location += "." + fn.func_name
                except:
                    pass
                probe_util.start(options['probeid'], location)

            is_debug = options.get('debug', "")
            if is_debug and DEBUG:
                import pydevd
                pydevd.settrace(options['debug'])

            # heartbeat_stop_event = threading.Event()
            # thread = threading.Thread(target=heartbeat_main, args=(t, task_db, heartbeat_stop_event))
            # thread.start()

            profile_type = options.get('profile', False)
            if profile_type:
                ret_value = {}
                if profile_type == 'line_profile':
                    import line_profiler
                    profiler = line_profiler.LineProfiler()
                    fn1 = profiler(fn)
                    ret_value['ret'] = fn1(user_and_tenant, *args, **options)
                    profiler.dump_stats(get_profile_filename("lp_" + fn.__name__))
                else:
                    import cProfile
                    if 'profile_name' in options:
                        cprofile_filename = get_profile_filename(options['profile_name'])
                    elif t.framework_version == '2':
                        cprofile_filename = "cp_{}.profile".format(t.res_id)
                    else:
                        cprofile_filename = get_profile_filename("cp_" + fn.__name__)
                    cProfile.runctx('ret_value["ret"]=fn(user_and_tenant, *args, **options)',
                                    locals(), globals(),
                                    filename=cprofile_filename)
                return ret_value['ret']
            else:
                fn_response = fn(user_and_tenant, *args, **options)
                """ if response is a dictionary, the the large response is stored in S3, and the key is passed around """
                return check_response_size(fn_response, user_and_tenant, str(tracer.trace))

        except GnanaError as ge:
            # Do the house keeping first.  Saving error state may
            # fail too :-(
            failed = True
            logger.error("Task (%s) Failed to execute with GnanaError: %s \n",
                         celery_id, ge.details,
                         exc_info=sys.exc_info())

            # We are fetching again to account for any updates by
            # the called tasks during their execution.
            t = task_db.fetch_task(celery_id)
            if t:
                t.stop_time = date_utils.now()

                exc_type, exc_value, exc_traceback = sys.exc_info()
                err_msg = "TASK GNANAERROR: " + str(traceback.format_exception(exc_type, exc_value, exc_traceback))

                t.current_status_msg = err_msg
                t.status = Task.STATUS_ERROR
                memory_use = memory_usage_resource()
                t.mem_consumed = memory_use
                task_db.save_task(t, field_list=['object.status',
                                                    'object.current_status_msg',
                                                    'object.stop_time',
                                                    'object.mem_consumed'])
                TaskArchive.archive_data(t)
            raise Exception(
                "%s" % json.dumps(ge.details) if ge.details else "GnanaError raised.")

        except BaseException as ex:
            # Do the house keeping first.  Saving error state may

            # fail too :-(
            failed = True
            logger.error("Task (%s) Failed to execute", celery_id, exc_info=sys.exc_info())

            # We are fetching again to account for any updates by
            # the called tasks during their execution.
            t = task_db.fetch_task(celery_id)
            trace_id = None
            if t is not None:
                t.stop_time = date_utils.now()
                exc_type, exc_value, exc_traceback = sys.exc_info()
                err_msg = "TASK EXCEPTION:  " + str(ex.message) + " "  + str(traceback.format_exception(exc_type, exc_value, exc_traceback))

                t.current_status_msg = err_msg
                t.status = Task.STATUS_ERROR
                memory_use = memory_usage_resource()
                t.mem_consumed = memory_use

                task_db.save_task(t, field_list=['object.status', 'object.current_status_msg',
                                        'object.stop_time', 'object.mem_consumed'])
                task_db.archive(t)

            else:
                logger.error('Unable to find the task.  Perhaps it is cleaned while the task is running.')

            trace_id = None
            try:
                trace_id = t.trace
            except:
                pass
            raise Exception("%s, Trace_id: %s" % (str(ex), trace_id))
        finally:

            # if heartbeat_stop_event is not None:
            #     heartbeat_stop_event.set()

            if options.get('profile', False):
                with open(cprofile_filename, 'r') as f:
                    final_name = '/'.join(["profiles", CNAME,
                                           sec_context.name,
                                           tracer.trace])
                    gnana_storage.add_file_object(f, final_name,
                                                  filename=cprofile_filename,
                                                  replace=True,
                                                  bucket=gnana_cprofile_bucket)

            end_cpu_times = process.cpu_times()
            end_time = time.time()
            memory_use, cpu_perc, time_taken = get_stats(start_time, end_time, starting_cpu_times, end_cpu_times)

            if not failed:
                # We are fetching again to account for any updates by
                # the called tasks during their execution.
                t = task_db.fetch_task(celery_id)
                if t:
                    if t.v2_run_type == 'priority_run':
                        memory_use = memory_use - start_memory
                    t.stop_time = date_utils.now()
                    t.status = Task.STATUS_FINISHED
                    t.current_status_msg = 'completed'
                    t.mem_consumed = memory_use
                    task_db.save_task(t, field_list=['object.stop_time', 'object.status',
                                                     'object.current_status_msg', 'object.mem_consumed'])
                    task_db.archive(t)
                    if t.framework_version == '2':
                        V2StatsLog.addv2statslog(t.type, memory_use, cpu_perc, time_taken, t.v2_run_type)
                path = ""
                if t:
                    try:
                        path = t.type.split(".")[-1]
                    except:
                        pass
                logger.info("Task (%s) completed successfully. %s" % (celery_id,  path))

            if t:
                task_metrics.tags["task.status"] = t.status_str()
                task_metrics.set_counter("task.mem", memory_use)
                task_metrics.set_counter("task.cpu", cpu_perc)
                task_metrics.set_counter("task.retries", t.retry_count or 0)
                task_metrics.set_timer("task.wait_time",
                                       (datetime.utcnow() - t.created_datetime).total_seconds() * 1000)

                logger.metrics("Task Executed {0}".format(celery_id), metrics=task_metrics)

            # If we are probing end it
            probe_util.end()
            if old_trace:
                tracer.trace = old_trace
            sec_context.set_context(*oldname)

    return new_task

gnana_task_support.thread_local_tags = thread_local_tags

def get_stats(start_time, end_time, starting_cpu_times, end_cpu_times):
    time_taken = end_time - start_time
    psys_time = end_cpu_times.system - starting_cpu_times.system
    puser_time = end_cpu_times.user - starting_cpu_times.user
    cpu_perc = ((psys_time + puser_time)/(time_taken))*100
    memory_use = memory_usage_resource()
    return (memory_use, cpu_perc, time_taken)


"""
If the response size is big, thenstore the respnse in S3 and send the key back, so during reading the response,
the json is loaded from the key.
"""


def check_response_size(fn_response, user_and_tenant, trace):
    try:
        length = len(str(fn_response))
        if length > pow(2, 22):
            key_prefix = '/'.join([user_and_tenant[0],
                                   user_and_tenant[1], trace])
            directory_name = tempfile.mkdtemp(str(tracer.trace), dir=TMP_DIR)
            filename = str(uuid.uuid4()) + '_large_resp.tmp'
            with open(os.path.join(directory_name, filename), 'w') as fp:
                fp.write(json.dumps(fn_response))
            file_utils.upload_to_s3(directory_name, filename, key_prefix)
            logger.info(
                "length of response is %s which is more than mongodb max doc size and response is stored in s3_key %s",
                length, key_prefix)
            return {large_response_key: key_prefix, filename_key: filename}
    except Exception as e:
        raise Exception("got error while checking response_size: %s" % e)
    return fn_response


def load_response(resp_dict):
    directory_name = tempfile.mkdtemp(dir=TMP_DIR)
    try:
        """ We expect that the large_response_key and filename are part of the resp_dict, this will be populated
        in case of a large response during task processing """
        filename = resp_dict[filename_key]
        key = resp_dict[large_response_key]
        logger.info("loading response from %s-%s" % (key, filename))
        file_utils.download_from_s3(directory_name, filename, key)
        with open(os.path.join(directory_name, filename), 'r') as f:
            return json.load(f)
    except Exception as ex:
        msg = 're-raising exception %s while loading response of %s - %s' % (
            key, filename, ex)
        logger.warning(msg)
        raise GnanaError(msg)
    finally:
        shutil.rmtree(directory_name)


def get_profile_filename(prefix=""):
    return "%sprofile_%s_%s.profile" % (prefix, os.getpid(), time.time())


class UnknownTaskError(Exception):

    """ Raised when the completed task is not found  to be pending"""
    pass


def revokejobs_streaming(criteria, cleanup=False, reset_chipotle_trace=False):
    try:
        yield '{"affected_tasks":['
        comma = ""
        success = "true"
        tasklist = defaultdict(dict)
        status = ''
        status_criteria = {'object.status': {'$in': [Task.STATUS_CREATED,
                                                     Task.STATUS_STARTED,
                                                     Task.STATUS_SUBMITTED,
                                                     Task.STATUS_ERROR]}}
        criteria = {'$and': [criteria, status_criteria]}
        msg = ''
        total_task_count = 0
        field_list = {'object.extid': 1, 'object.tenant': 1,
                      'object.framework_version': 1, 'object.trace': 1}
        if cleanup:
            field_list['object.task_meta'] = 1
            field_list['object.tasktype'] = 1
        for task in gnana_db.findDocuments(Task.getCollectionName(), criteria,
                                           fieldlist=field_list,
                                           read_from_primary=True,
                                           tenant_aware=Task.tenant_aware):
            yield '%s"%s"' % (comma, task['object']['extid'])
            comma = ","
            total_task_count += 1
            if task['object']['framework_version'] == '2' and cleanup:
                task_details = (task['object']['extid'], task['object']['task_meta'],
                                task['object']['tasktype'], task['object']['trace'])
            else:
                task_details = (task['object']['extid'], task['object']['trace'])
            if task['object']['tenant'] in tasklist:
                if task['object']['framework_version'] in tasklist[task['object']['tenant']]:
                    tasklist[task['object']['tenant']][
                        task['object']['framework_version']].append(task_details)
                else:
                    tasklist[task['object']['tenant']][task['object']['framework_version']] = [task_details]
            else:
                tasklist[task['object']['tenant']][task['object']['framework_version']] = [task_details]
        progress = 0
        for tenant, framework_versions in tasklist.iteritems():
            user_name = 'revoke'
            logintenant = 'administrative.domain'
            sec_context.set_context(user_name, tenant, logintenant, user_name, 'tenant', {})
            trace_list = []
            for version, total_tasks in framework_versions.iteritems():
                if msg:
                    msg += ' and '
                msg += str(len(total_tasks)) + " v" + version + "_tasks"
                logger.info("Retrieved %s v%s_tasks for revoking for %s" % (str(len(total_tasks)), version, tenant))
                if version == '1':
                    total_tasks_ = []
                    for i, task_info in enumerate(total_tasks):
                        task_id, trace = task_info
                        total_tasks_.append(task_id)
                        if trace not in trace_list:
                            trace_list.append(trace)
                        progress = check_and_log_progress(i, total_task_count, progress)
                        res = AsyncResult(task_id)
                        if not res.ready():
                            id_ = task_id
                            logger.error(
                                "terminating task %s, as one of the sub_tasks failed in this group", task_id)
                            celery.task.control.revoke(id_, terminate=True, signal='SIGKILL')
                    set_to_criteria = {'$set': {'object.status': Task.STATUS_ERROR,
                                                'object.current_status_msg': "revoked"}}
                    task_criteria = {'object.extid': {'$in': total_tasks_}}
                    gnana_db.updateAll(Task.getCollectionName(), task_criteria, set_to_criteria)
                else:
                    total_tasks_ = []
                    for i, taskdetail in enumerate(total_tasks):
                        if cleanup:
                            task_id, task_meta, path, trace = taskdetail
                            params = task_meta['params']
                            context = task_meta['context']
                            from aviso.framework.tasker import V2Task
                            v2task_obj = V2Task.create_task(path, params=params, context=context)
                            v2task_obj.cleanup_on_revoke()
                        else:
                            task_id, trace = taskdetail
                        total_tasks_.append(task_id)
                        if trace not in trace_list:
                            trace_list.append(trace)
                    task_criteria = {'object.extid': {'$in': total_tasks_}}
                    total_tasks = total_tasks_
                    set_to_criteria = {'$set': {'object.status': Task.STATUS_TERMINATED,
                                                'object.current_status_msg': "revoked"}}
                    gnana_db.updateAll(Task.getCollectionName(), task_criteria, set_to_criteria)
                    TaskActive.truncate_or_drop(task_criteria)
                    criteria = {'object.requesting_task': {'$in': total_tasks}}
                    set_to_criteria = {'$set': {'object.status': ResultCache.FAILED}}
                    gnana_db.updateAll(ResultCache.getCollectionName(), criteria, set_to_criteria)
                    progress = check_and_log_progress(len(total_tasks), total_task_count, progress)
            msg += " for " + tenant
            msg = " Retrieved " + msg + " for revoking"
            status = ',"status":"%s"' % msg
            if reset_chipotle_trace:
                chipotle_status = sec_context.details.get_flag('molecule', 'chipotle_trace', 'finished')
                if chipotle_status in trace_list:
                    sec_context.details.set_flag('molecule', 'chipotle_trace', 'finished')
                dtfo_trace = sec_context.details.get_flag('molecule', 'dtfo_trace', 'finished')
                if dtfo_trace in trace_list:
                    sec_context.details.set_flag('molecule', 'dtfo_trace', 'finished')
                load_activity_trace = sec_context.details.get_flag('molecule', 'load_activity_trace', 'finished')
                if load_activity_trace in trace_list:
                    sec_context.details.set_flag('molecule', 'load_activity_trace', 'finished')
                snapshot_trace = sec_context.details.get_flag('molecule', 'snapshot_trace', 'finished')
                if snapshot_trace in trace_list:
                    sec_context.details.set_flag('molecule', 'snapshot_trace', 'finished')
        logger.info("Revoke progress: %%100.")
        if not tasklist:
            msg = "Retrieved 0 tasks for revoking"
            status += ',"status":"%s"' % msg
            logger.info(msg)
        yield '],"success":%s%s}' % (success, status)
    except Exception as e:
        logger.exception(e.message)


def check_and_log_progress(tasks_revoked, total_task_count, progress):
    this_progress = float(tasks_revoked) / total_task_count
    if this_progress - progress > 0.1:
        logger.info("Revoke progress: %%%.0f", this_progress * 100.)
        progress = this_progress
    return progress


def revokejobs(traceid):
    return json.loads(''.join(revokejobs_streaming(traceid)))
