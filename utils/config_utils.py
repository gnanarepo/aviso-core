import logging
from aviso.settings import sec_context

import copy
import bson
from utils.constants import PRIVATE_MOXTRA_URL, PUBLIC_MOXTRA_URL

logger = logging.getLogger('gnana.%s' % __name__)


def config_pattern_expansion(attrs):
    tdetails = sec_context.details
    eval_errs = {}

    def get_keys(attrs, item, path, mainpath):
            if isinstance(item, dict):
                mainpath = path
                for key in item.keys():
                    path = mainpath + "['" + str(key) + "']"
                    get_keys(attrs, item[key], path, mainpath)
            elif hasattr(item, '__iter__'):
                mainpath = path
                for i in item:
                    if isinstance(i, dict):
                        get_keys(attrs, i, path, mainpath)
                    else:
                        expr = copy.copy(item)
                        err = checkpattern(attrs, i, path, "list", in_and_ex=item)
                        if err:
                            eval_errs[path[path.find('[') + 2:-2]] = [expr, err]
            elif not hasattr(item, '__iter__'):
                err = checkpattern(attrs, item, path, "dict", in_and_ex=None)
                if err:
                    eval_errs[path[path.find('[') + 2:-2]] = [item, err]
#                 eval_errs[path] = checkpattern(attrs, item, path, "dict", in_and_ex=None)

    def checkpattern(attrs, i, path, i_type, in_and_ex):
        if isinstance(i, str):
            if i.startswith(("$(", "${", "$[")) and '/' in i[2:]:
                pattern_type = i[1:2]
                category_key, config_name = i[2:-1].split("/")
                pattern_value = None
                pattern_value = copy.copy(tdetails.get_config(category_key, config_name, None))
                if not pattern_value:
                    return ["Invalid expression in configuration. Value cannot be derived from Tenant config."]
                if isinstance(pattern_value, dict) and pattern_type != "{":
                    return "Pattern mismatch in configuration.Pattern is %s" % i
                elif isinstance(pattern_value, list) and pattern_type != "[":
                    return "Pattern mismatch in configuration.Pattern is %s" % i
                last_item = path.rfind("[")
                if i_type == 'dict':
                    last_item = path.rfind("[")
                    eval(path[:last_item])[path[last_item + 2:-2]] = pattern_value
                elif i_type == 'list':
                    in_and_ex_err = []
                    if len(in_and_ex) > 3 and (isinstance(in_and_ex, list) or isinstance(in_and_ex, tuple)):
                        in_and_ex_err.append("Incorrect expression. Expression must be in format 'expression, " +
                                             "inclusion list or map, exclusion list'")
                    if isinstance(pattern_value, list):
                        if len(in_and_ex) == 3:
                            for every in in_and_ex[2]:
                                if every in pattern_value:
                                    pattern_value.remove(every)
                                else:
                                    in_and_ex_err.append(" %s is not present in Tenant config" % every)
                        for every_value in in_and_ex[1]:
                            if every_value in pattern_value:
                                in_and_ex_err.append("%s should be in exclusion list as you are trying" % every_value +
                                                     " to modify the overridden Tenant config")
                            pattern_value.append(every_value)
                    if isinstance(pattern_value, dict):
                        if len(in_and_ex) == 3:
                            for every in in_and_ex[2]:
                                if every in pattern_value.keys():
                                    pattern_value.pop(every)
                                else:
                                    in_and_ex_err.append(" %s is not present in Tenant config" % every)
                        for every_key in in_and_ex[1].keys():
                            if every_key in pattern_value.keys():
                                in_and_ex_err.append(" %s should be in exclusion list as you are " % every_key +
                                                     "trying to modify the overridden Tenant config")
                            pattern_value[every_key] = in_and_ex[1][every_key]
                    if in_and_ex_err:
                        return in_and_ex_err
                    del(eval(path)[1:])
                    eval(path[:last_item])[path[last_item + 2:-2]] = pattern_value
        return None
    get_keys(attrs, attrs, 'attrs', '')
    return {'attrs': attrs, 'eval_errs': eval_errs}


def err_format(eval_errors):
    if not eval_errors:
        return '\n'
    errstr = '\n \nThese are the fields bypassed by the evaluation due to save_on_error . \n '
    for key in eval_errors.keys():
        errstr = errstr+'destination = '+key+'\n'
        errstr = errstr+'expression = '+str(eval_errors[key][0])+'\n'
        errstr = errstr+'message_list = '+str(eval_errors[key][1])+'\n\n'
    return errstr


def get_max_allowed_batch_size(list_of_records, model):
    model_config = sec_context.details.get_config('compressed_model', model, dict())
    if not model_config:
        b = bson.BSON()
        size = len(b.encode(dict(list_of_records=list_of_records)))
        num = len(list_of_records)
        max_size = 16 * 1024 * 1024
        if size < max_size:
            batch = num
        else:
            safefy_limit = 0.9
            single = (size * 1.0) / num
            batch = int((max_size / single) * safefy_limit)
        model_config = dict(max_allowed = batch)
        try:
            sec_context.details.set_config('compressed_model', model, model_config)
        except Exception as e:
            logger.warning("warn: failed to set model %s config %s", model, e)
        try:
            sec_context.details.set_config('compressed_model', "migrated", True)
        except Exception as e:
            logger.warning("warn: failed to set Migrated config %s", e)
    return model_config.get('max_allowed', 10000)


def get_moxtra_url(guest=None, domain=None):
    is_moxtra_public = False
    try:
        is_moxtra_public = sec_context.details.get_config('collaboration',
                                                          'is_public',
                                                          False)
    except Exception as mex:
        if guest and domain and ".aviso.com" in domain:
            try:
                tenant_splt = domain.split(".aviso.com")[0].split(".")
                if len(tenant_splt) == 2:
                    tenant_name = tenant_splt[0].replace("-", "_")
                    from domainmodel.tenant import Tenant
                    tenants_list = Tenant.getDistinctValues("name")
                    for t_name in tenants_list:
                        if t_name.startswith(tenant_name):
                            sec_context.set_context("local_cache", t_name, t_name)
                            is_moxtra_public = sec_context.details.get_config('collaboration', 'is_public', False)
                            break
            except:
                pass
    config_private_url = None
    config_public_url = None
    if not guest:
        try:
            from config.fm_config import FMConfig
            fm_config = FMConfig()
            config_private_url = fm_config.get_moxtra_private_url
            config_public_url = fm_config.get_moxtra_public_url
        except:
            pass
    if is_moxtra_public:
        if config_public_url:
            moxtra_url = config_public_url
        else:
            moxtra_url = PUBLIC_MOXTRA_URL
    else:
        if config_private_url:
            moxtra_url = config_private_url
        else:
            moxtra_url = PRIVATE_MOXTRA_URL
    return moxtra_url
