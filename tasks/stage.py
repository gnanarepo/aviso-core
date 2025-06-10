import copy
import json
import logging
import os
import pprint
import sys
import tempfile
import time
import traceback

import celery
from aviso.settings import (CNAME, CNAME_DISPLAY_NAME, EMAIL_SENDER, gnana_db,
                            gnana_storage, sec_context)

from domainmodel import datameta
from tasks import dataload, gnana_task_support
from utils import GnanaError, diff_rec
from utils.config_utils import config_pattern_expansion, err_format
from utils.file_utils import gitbackup_dataset
from utils.mail_utils import send_mail2

logger = logging.getLogger('gnana.%s' % __name__)

CONFIG_PATHS_FULL_PREPARE = ['params.general.indexed_fields',
                             'params.general.dimensions',
                             'maps']


def capture_paths(diff, path):

    path_set = set()

    def capture_paths_internal(diff, path, path_set):

        try:
            for i in diff:
                if i == 'ValDiff':
                    capture_paths_internal(diff[i], path, path_set)
                elif i == 'OnlyInRight' or i == 'OnlyInLeft' or isinstance(diff, tuple):
                    path_without_trailing_dot = path[:-1]
                    path_set.add(path_without_trailing_dot)
                    y = path_without_trailing_dot.split('.')
                    path = '.'.join(y[:-1])
                    path = path + '.' if len(y) > 1 else path
                    if isinstance(diff, tuple):
                        break
                elif isinstance(i, int):
                    path_without_trailing_dot = path[:-1]
                    path_set.add(path_without_trailing_dot)
                else:
                    path += (i+'.')
                    path = capture_paths_internal(diff[i], path, path_set)
            return path
        except Exception as _:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            err_msg = "\n".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
            logger.error('Error while calculating the paths internally .\n%s', err_msg)

    capture_paths_internal(diff, path, path_set)

    return path_set


def validate_expression(ds, stage, save_on_error):
    attrs = {}
    ds.encode(attrs)
    attr = copy.deepcopy(attrs)
    res = config_pattern_expansion(attr)
    eval_errors = res.get('eval_errs', None)
    tdetails = sec_context.details
    if eval_errors:
        if save_on_error == 'True':
            if tdetails.get_flag('save_on_error', ds.name+'~'+stage, {}):
                tdetails.remove_flag('save_on_error', ds.name+'~'+stage)
            tdetails.set_flag('save_on_error', ds.name, eval_errors.keys())
        else:
            raise GnanaError(eval_errors)
    else:
        if tdetails.get_flag('save_on_error', ds.name+'~'+stage, {}):
            tdetails.remove_flag('save_on_error', ds.name+'~'+stage)
        if tdetails.get_flag('save_on_error', ds.name, {}):
            tdetails.remove_flag('save_on_error', ds.name)
    ds.decode(attrs)
    ds.save()
    return eval_errors


def backup_dataset(username, ds, inbox_name):
    attrs = {}
    stackspecific = ds.params['general'].get('stackspecific', False)
    ds.encode(attrs)
    for ft in attrs['filetypes']:
        try:
            del ft['last_approved_time']
        except KeyError:
            pass
    backup = json.dumps(ds.get_as_map())
    fp = tempfile.NamedTemporaryFile(prefix='dataset', delete=False)
    fp.write(backup)
    fp.close()
    gnana_storage.add_adhoc_file(username,
                                 "_".join([inbox_name, str(time.time())]),
                                 fp.name, dataset=ds.name, stackspecific=stackspecific)
    os.unlink(fp.name)


@celery.task
def purge_stage(user_and_tenant, **args):
    return purge_stage_task(user_and_tenant, **args)


@gnana_task_support
def purge_stage_task(user_and_tenant, **args):
    """
    Purge all the data (from file system, dataset, and results, if any)
    for the specified data set and stage.
    """

    dataset = args.get('dataset')
    inbox = args.get('stage')
    retain_partiton = args.get('retain_partition', False)
    ds = datameta.Dataset.getByName(dataset)
    stackspecific = ds.params['general'].get('stackspecific', False)

    # 1. Remove the files in the stage
    filelist = gnana_storage.filelist(sec_context.name,
                                      inbox=inbox,
                                      dataset=dataset,
                                      stackspecific=stackspecific)
    for filedef in filelist:
        logger.debug("File def %s",  filedef)
        if filedef.filetype == 'partitions' and retain_partiton:
            continue
        try:
            gnana_storage.deleteFile(sec_context.name, inbox, dataset,
                                     filedef.filetype, filedef.name,
                                     stackspecific=stackspecific)
        except OSError:
            continue

    # 2. Remove any stage entry in the dataset
    datameta.Dataset.deleteStageData(dataset, inbox)

    # 3. Delete the InboxEntry (Staged Data) Collection
    # Delete stage data in inbox collections
    InboxClass = datameta.InboxEntryClass(dataset, inbox)
    gnana_db.dropCollection(InboxClass.getCollectionName())

    # 4. Delete the Staged Results collections
    result_prefix = ".".join([user_and_tenant[1], dataset, "_results"])
    for collection_name in gnana_db.collection_names(prefix=result_prefix):
        if collection_name.endswith(".%s" % inbox):
            gnana_db.dropCollection(collection_name)

    # 5. Delete the staged rejections related collections
    result_prefix = ".".join([user_and_tenant[1], dataset, "_rej"])
    for collection_name in gnana_db.collection_names(prefix=result_prefix):
        if collection_name.endswith(".%s" % inbox):
            gnana_db.dropCollection(collection_name)


def ds_stage_config(user_and_tenant, inbox, ds, uip_merge=False, **args):
    git_errors = None
    save_on_error = args.get('save_on_error', False)
    dataset = args.get('dataset')
    auto_purge = args.get('auto_purge')
    stage = args.get('stage')
    dstage = datameta.Dataset.getByNameAndStage(dataset, inbox, full_config=False)
    mail_changes = diff_rec(ds.get_as_map(), dstage.get_as_map(), {'stages'})
    try:
        paths_changed = list(capture_paths(mail_changes, ''))
        logger.info('Paths changed %s' % paths_changed)
        mark_full_prepare = False
        for path in paths_changed:
            for prepare_path in CONFIG_PATHS_FULL_PREPARE:
                if path.startswith(prepare_path):
                    logger.info('As path %s changed. Marking for full_prepare ..' % path)
                    mark_full_prepare = True
                    break
            if mark_full_prepare:
                break

        if mark_full_prepare:
            logger.info("Saving the full_prepare flag")
            tdetails = sec_context.details
            prep_status = tdetails.get_flag('prepare_status', ds.name, {})
            prep_status['full_prepare'] = mark_full_prepare
            tdetails.set_flag('prepare_status', ds.name, prep_status)
    except Exception as _:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        err_msg = "\n".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
        logger.error('Error while calculating the paths.\n%s', err_msg)

    dataset_changes = ds.stages.get(inbox, {})
    creator = dataset_changes.get('creator', args['approver'])
    comment = dataset_changes.get('comment', "Modified " + dataset + " config")
    gitcomment = (comment +
                  "-Creator:" + creator +
                  " -Approver:" + args['approver'])
    file_name = dataset + ".json"

    # sending mail
    if dataset_changes:
        eval_errs = validate_expression(ds, stage, save_on_error)
        status = gitbackup_dataset(file_name, dstage.get_as_map(), gitcomment)
        git_errors = status[1]
        cname = CNAME_DISPLAY_NAME if CNAME_DISPLAY_NAME else CNAME
        if status[0]:
            format_changes = pprint.pformat(mail_changes, indent=2)
            tdetails = sec_context.details
            send_mail2('dataset_changes.txt',
                       'Aviso <notifications@aviso.com>',
                       tdetails.get_config('receivers', 'dataset_changes', ['gnackers@aviso.com']),
                       reply_to='Data Science Team <gnackers@aviso.com>',
                       dataset=dataset, tenantname=sec_context.name,
                       comment=comment, changes=format_changes + err_format(eval_errs),
                       creator=creator, approver=args['approver'], cName=cname, creator_name=creator.rsplit('@')[0])
            logger.info('Sending dataset changes mail for dataset:'
                        ' %s, tenant: %s, user: %s' %
                        (dataset, sec_context.name, sec_context.user_name))

    logger.info("Approving the stage %s", inbox)

    # 0. Save the encoded dataset into S3 storage before merging
    backup_dataset(user_and_tenant[1], ds, inbox)

    # 1. Merge the dataset and save back
    ds.merge_stage(inbox, delete_stage=True)
    ds.save()
    if uip_merge:
        stackspecific = ds.params['general'].get('stackspecific', False)
        pd = ds.get_partition_data(stage=inbox)

        # Add the dataset class for further processing
        ds.DatasetClass = datameta.DatasetClass(ds)
        # 2. Merge the inbox entries
        # Calling this method will take care of distributing the work to
        # multiple workers
        dataload.merge_inbox_entries_to_uip(user_and_tenant, dataset, inbox,
                                            auto_purge=False)
        # 3. Move the files in S3 from the current stage into _data
        for f in gnana_storage.filelist(user_and_tenant[1], inbox=inbox, dataset=dataset, stackspecific=stackspecific):
            if f.filetype == 'partitions':
                continue
            logger.info("Moving file %s in area %s", f.fullpath, f.filetype)
            gnana_storage.move_file(user_and_tenant[1], "_data", dataset, f.filetype,
                                    f.name,
                                    f.fullpath, stackspecific=stackspecific)
    # 4. Auto purge
    if auto_purge:
        purge_stage(user_and_tenant, retain_partition=True, **args)
        if pd:
            logger.info("Saving the partition data back")
            pd.save()
            ds.stages['inbox'] = {}
            ds.save()

    # 5. Update the data_update flag
    logger.info("Saving the data_update flag")
    tdetails = sec_context.details
    tdetails.set_flag('data_update', dataset, time.time())

    return git_errors
