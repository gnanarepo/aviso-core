import copy
import time

from utils.mail_utils import backup_and_mail_changes
from aviso.settings import CNAME, CNAME_DISPLAY_NAME, gnana_db2, sec_context

from domainmodel import datameta
from domainmodel.tenant import tenant_events
from tasks.stage import ds_stage_config
from tasks.targetspecTasks import target_spec_tasks
from utils.mail_utils import send_mail2


class Feature:
    ui_name = 'FeatureX'
    ui_description = 'This will allow you to do configuration changes.'
    category = "Misc"

    """ If you want to order your feature in UI, update it here """
    ui_feature_order = ("Forecast Management", "Data Science", "ETL & Integration", "Reports", "Misc")

    def __init__(self):
        self.host = 'https://'+(CNAME_DISPLAY_NAME if CNAME_DISPLAY_NAME is not None else CNAME)+'.aviso.com'
        self.all_config = None
        self.all_datasets_config = {}
        self.all_target_spec = {}

    def getModes(self):
        return self.modes.keys()

    def getModeDescription(self, featuremode):
        return self.modes[featuremode]

    def getCurrentMode(self):
        raise Exception('Must be implemented by subclass')

    def getRequiredInputs(self, featuremode):
        raise Exception('Must be implemented by subclass')

    def getCurrentConfig(self, featuremode):
        raise Exception('Must be implemented by subclass')

    def getRestrictedModes(self, all_feature_modes):
        return {}

    def getRequiredJobDetails(self,featuremode):
        return {}

    def configure(self, featuremode, **kwargs):
        @tenant_events('Configure '+self.ui_name+' in '+featuremode+ "(" +kwargs['comment']+")", 'blue')
        def run_configuration():
            return self.do_configuration(featuremode, kwargs['config'], kwargs['comment'])
        return run_configuration()

    def get_nested_config(self, nesting, default=None):
        if not self.all_config:
            t_details = sec_context.details
            self.all_config = copy.deepcopy(t_details.get_all_config())

        config_dict = copy.deepcopy(self.all_config)
        for x in nesting[:-1]:
            config_dict = config_dict.get(x, {})

        return config_dict.get(nesting[-1], default)

    def commit_tenant_config_changes(self, change_list, comments):
        # Commit the tenant config changes
        # To remove a config_name key append (category, config_name) to change list
        # To set config to a config_name key append (category, config_name, config ) to change_list

        if not change_list:
            return
        t_details = sec_context.details
        for each_change in change_list:
            each_change_length = len(each_change)
            if each_change_length == 2:
                if t_details.get_config(each_change[0], each_change[1]):
                    t_details.remove_config(each_change[0], each_change[1])
            elif each_change_length == 3:
                t_details.set_config(each_change[0], each_change[1], each_change[2])
            else:
                raise Exception('change_list is not good')

        # Backup and notify
        tenant_name = sec_context.name
        tenant_name = tenant_name.lower()
        current_user_name = sec_context.user_name
        current_user_name = current_user_name.lower()
        new_tenant_details = t_details.get_all_config()
        backup_and_mail_changes(self.all_config, new_tenant_details, tenant_name, current_user_name, comments,
                                'Configure_UI')

    def get_dataset(self, ds_type):
        known_datasets = []
        known_dataset = gnana_db2.findDocuments(datameta.Dataset.getCollectionName(), {'object.ds_type': ds_type}, {})
        for kd in known_dataset:
            known_datasets.append(kd['object']['name'])
        return known_datasets

    def get_dataset_nested_config(self, **kwargs):
        dataset = kwargs.get('dataset', 'OppDS')
        stage = kwargs.get('stage', None)
        path = kwargs.get('path', None)
        default_ret = kwargs.get('default_return_val', None)
        if not self.all_datasets_config.get(dataset, None):
            ds = datameta.Dataset
            self.all_datasets_config[dataset] = copy.deepcopy(ds.getByNameAndStage(dataset, stage).get_as_map())
        ds_config_dict = copy.deepcopy(self.all_datasets_config[dataset])

        nesting = path.split('.')
        for config_key in nesting[:-1]:
            ds_config_dict = ds_config_dict.get(config_key, {})

        return ds_config_dict.get(nesting[-1], default_ret)

    def commit_dataset_config_changes(self, dataset, stage, change_list, comments):
        """
        dataset: dataset name
        stage: stage name
        change_list: a list of tuples where each tuple contains action, path and new_value
                    **if action is 'remove_path' then we should pass new_value as None
        comments: comments
        """
        if not change_list:
            return
        ds = datameta.Dataset.getByName(dataset)
        ds.stages[stage] = {'changes': [],
                            'comment': comments}
        for each_change in change_list:
            ds.apply(stage, each_change[0], each_change[1], each_change[2])
        ds.save()

        # Backup
        tenant_name = sec_context.name
        tenant_name = tenant_name.lower()
        current_user_name = sec_context.user_name
        current_user_name = current_user_name.lower()
        ds_stage_config([current_user_name, tenant_name], stage, ds, uip_merge=False, dataset=dataset,
                        stage=stage, approver=current_user_name)

    def get_target_spec_nested_config(self, target_spec, nesting, default=None):
        if not self.all_target_spec.get(target_spec, None):
            ts = datameta.TargetSpec.getByName(target_spec)
            if not ts:
                self.all_target_spec[target_spec] = ts
                return default
            self.all_target_spec[target_spec] = copy.deepcopy(ts.encode({}))
        if not nesting:
            return copy.deepcopy(self.all_target_spec[target_spec])

        ts_config_dict = copy.deepcopy(self.all_target_spec[target_spec])
        for x in nesting[:-1]:
            ts_config_dict = ts_config_dict.get(x, {})

        return ts_config_dict.get(nesting[-1], default)

    def commit_target_spec_changes(self, target_name, action, comments, new_vlaue, module_path=None):
        current_user_name = sec_context.user_name
        target_spec = datameta.TargetSpec.getByFieldValue('name', target_name)
        old_target_spec = {}
        if target_spec:
            old_target_spec = copy.deepcopy(target_spec.__dict__)
        target_spec_tasks(current_user_name, target_name, new_vlaue, False, action, old_target_spec,
                          module_path, comments)

    def get_tenant_endpoint(self, endpoint_name):
        tnt_details = sec_context.details
        try:
            cred = tnt_details.get_credentials(endpoint_name)
            return cred
        except:
            return None

    def set_tenant_endpoint(self, endpoint_name, creds):
        tnt_details = sec_context.details
        credentials = creds
        credentials['updated_time'] = int(time.time())
        credentials['sent_mail'] = False
        tnt_details.save_credentials(endpoint_name, credentials)
        tnt_details.save()
        cname = CNAME_DISPLAY_NAME if CNAME_DISPLAY_NAME else CNAME
        send_mail2(
            'endpoint_change.txt',
            'notifications@aviso.com',
            tnt_details.get_config('receivers', 'endpoint_changes', ['gnackers@aviso.com']),
            reply_to='Data Science Team <gnackers@aviso.com>',
            tenantname=sec_context.name, cName=cname,
            endpoint=endpoint_name,
            modifier=sec_context.user_name, user_name=sec_context.user_name,
        )
        try:
            cred = tnt_details.get_credentials(endpoint_name)
            return cred
        except:
            return None
