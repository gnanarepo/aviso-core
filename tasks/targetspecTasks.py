import logging
import pprint

from aviso.settings import sec_context, gnana_db, CNAME, CNAME_DISPLAY_NAME

from domainmodel.datameta import TargetSpec
from tasks.stage import validate_expression
from utils import diff_rec
from utils.config_utils import err_format
from utils.file_utils import gitbackup_dataset
from utils.mail_utils import send_mail2

logger = logging.getLogger('gnana.%s' % __name__)


def verifyAndValidate(payload, target_name, save_on_error, old_target_spec=None, action=None):
    target_spec = TargetSpec()
    target_spec.name = target_name
    target_spec.report_spec = payload.get('report_spec', {})
    target_spec.drilldowns = payload.get('drilldowns', [])
    target_spec.module = payload.get('module', "")
    target_spec.models = payload.get('models', {})
    target_spec.keys = payload.get('keys', {})
    target_spec.task_v2 = payload.get('task_v2', False)
    eval_errors = validate_expression(target_spec, save_on_error, old_target_spec, action)
    return eval_errors


def get_attr_or_item(c, i, default):
    if isinstance(c, list):
        try:
            return c[i]
        except IndexError:
            return default
    if isinstance(c, dict):
        try:
            return c[i]
        except KeyError:
            return default
    return getattr(c, i, default)


def unset_attr_or_remove_item(c, i):
    if isinstance(c, dict):
        c.pop(i, None)
    else:
        delattr(c, i)


def set_attr_or_item(c, i, val):
    if isinstance(c, list) or isinstance(c, dict):
        c[i] = val
    else:
        setattr(c, i, val)


def target_spec_tasks(username, target_name, payload, save_on_error, action, old_target_spec=None, module_path=None,
                      commit_message=None):
    eval_errors = {}
    if action == 'create':
        eval_errors = verifyAndValidate(payload, target_name, save_on_error, old_target_spec, action)
    target_spec = TargetSpec.getByFieldValue('name', target_name)
    if action == 'update' or action == 'delete_module':
        module_segments = module_path.split('.')
        container = target_spec
        for x in module_segments[0:-1]:
            next_container = get_attr_or_item(container, x, None)
            if not next_container:
                set_attr_or_item(container, x, {})
                container = get_attr_or_item(container, x, None)
            else:
                container = next_container
        if action == 'update':
            set_attr_or_item(container, module_segments[-1], payload)
        else:
            unset_attr_or_remove_item(container, module_segments[-1])
        eval_errors = validate_expression(target_spec, save_on_error)
    elif action == 'purge_spec':
        tdetails = sec_context.details
        if tdetails.get_flag('save_on_error', target_name, {}):
                tdetails.remove_flag('save_on_error', target_name)
        tdetails.save()
        target_spec.remove(target_spec.id)
        gnana_db.dropCollectionsInNamespace("%s.combined_results.%s" % (sec_context.name, target_name))
        return {'success': True}
    elif action == 'recreate':
        eval_errors = verifyAndValidate(payload, target_name, save_on_error, old_target_spec, action)
        target_spec = TargetSpec.getByFieldValue('name', target_name)
    else:
        # Create is already handled
        pass

    attrs = {}
    target_spec.encode(attrs)
    # update into repository
    target = target_name
    new_target_spec = TargetSpec.getByFieldValue('name', target_name)
    comment = "Modified " + target + " config"
    gitcomment = (commit_message if commit_message else comment)
    gitcomment += (gitcomment + ' -user: ' + username)
    logger.info("Git comment : %s" % gitcomment)
    file_name = "targetspec_" + target + ".json"
    mail_changes = diff_rec(old_target_spec, new_target_spec.__dict__, {'id'})
    if mail_changes:
        status = gitbackup_dataset(file_name, new_target_spec.__dict__, gitcomment)
        if status[0]:
            format_changes = pprint.pformat(mail_changes, indent=2) + err_format(eval_errors)
            tdetails = sec_context.details
            cname = CNAME_DISPLAY_NAME if CNAME_DISPLAY_NAME else CNAME
            send_mail2('targetspec.txt',
                       'Aviso <notifications@aviso.com>',
                       tdetails.get_config('receivers', 'dataset_changes', ['gnackers@aviso.com']),
                       reply_to='Data Science Team <gnackers@aviso.com>',
                       target_name=target_name, tenantname=sec_context.name,
                       comment=comment, changes=format_changes,
                       user_name=sec_context.user_name, user=username, cname=cname)
    return attrs
