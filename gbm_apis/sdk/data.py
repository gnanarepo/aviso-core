"""The dataset shell verbs, ported from service-infrastructure/aviso/sdk/data.py.

Registered three times in shell.py -- as ``dataset``, ``uipmeta`` and
``sourcemeta`` -- differing only in the ``ds_type`` they send.

This runs on the client, inside the package preparesdk builds, so the paths are
written against the ``gbm/`` mount that aviso_core/urls.py gives this app.

Two commands the legacy class had are deliberately absent: ``list`` (DatasetList
was not ported, so GET /dataset has no view) and ``indexes`` (/index_list/<ds>
is not served here).
"""
import logging
from urllib.parse import urlencode

from ..interface.base import Argument, BaseSDKFunction, CommandMeta

logger = logging.getLogger()

BASE_PATH = '/gbm/dataset'

dataset = lambda: Argument(name='dataset',
                           arg_type=(str, dict),
                           required=False)
sandbox = lambda: Argument(name='sandbox',
                           arg_type=(str,))


class DatasetPythonSdkFunctions(BaseSDKFunction):

    default_command = 'dataset'

    meta_commands = {
        'dataset': CommandMeta(
            help='This command is to get datasets',
            name='dataset',
            arguments=[
                dataset(),
                sandbox(),
                Argument(name='file_type', arg_type=(str,)),
                Argument(name='map', arg_type=(str,)),
                Argument(name='model', arg_type=(str,)),
                Argument(name='param', arg_type=(str,)),
                Argument(name='field', arg_type=(str,)),
                Argument(name='full_config', arg_type=(bool,))
            ],
            url_pattern=None,
            validations=None),
        'getinfo': CommandMeta(
            help='This command is to get dataset info',
            name='getinfo',
            arguments=[
                dataset(),
                sandbox(),
                Argument(name='file_type', arg_type=(str,)),
                Argument(name='map', arg_type=(str,)),
                Argument(name='model', arg_type=(str,)),
                Argument(name='param', arg_type=(str,)),
                Argument(name='field', arg_type=(str,)),
                Argument(name='full_config', arg_type=(bool,))
            ],
            url_pattern=None,
            validations=None),
        'summary': CommandMeta(
            help='This command is to get summary',
            name='summary',
            arguments=[
                dataset(),
                sandbox()
            ],
            url_pattern=None,
            validations=None),
        'create': CommandMeta(
            help='This command is to create dataset',
            name='create',
            arguments=[
                dataset(),
                Argument(name='new_value',
                         arg_type=(str, dict, bool, list, set, tuple, int, float), required=True),
                Argument(name='save_on_error', arg_type=(bool,))
            ],
            url_pattern=None,
            validations=None),
        'purge': CommandMeta(
            help='This command is to purge dataset',
            name='purge',
            arguments=[
                dataset(),
                Argument(name='confirm', arg_type=(bool,))
            ],
            url_pattern=None,
            validations=None),
        'modify': CommandMeta(
            help='This command is to modify dataset',
            name='modify',
            arguments=[
                dataset(),
                sandbox(),
                Argument(name='action', arg_type=(str,), required=True),
                Argument(name='new_value', arg_type=(str, dict, bool, list, set, tuple, int, float)),
                Argument(name='path', arg_type=(str,), required=True),
                Argument(name='save_on_error', arg_type=(bool,))
            ],
            url_pattern=None,
            validations=None)
    }

    def __init__(self, ds_type='', *args, **kwargs):
        self.ds_type = ds_type
        super(DatasetPythonSdkFunctions, self).__init__(*args, **kwargs)

    def _get_base_url(self, shell, dataset=None, sandbox=None, suffix='',
                      **kwargs):
        ds_type = '?ds_type=' + self.ds_type
        url = BASE_PATH
        dataset = dataset or shell._dataset
        stage = sandbox or shell.entered_sandbox
        if dataset:
            url += '/{}'.format(dataset)
        if stage:
            url += '/{}'.format(stage)
        if suffix:
            url += '/{}'.format(suffix)
        return url + ds_type

    def dataset(self, shell, dataset=None, **kwargs):
        """The default command: fetch a dataset's config as stored.

        Builds the path directly rather than through _get_base_url, so no
        ds_type or full_config is sent. That is the legacy behaviour, and it
        means uipmeta(dataset=...) and dataset(dataset=...) are equivalent --
        use getinfo when the ds_type check matters.
        """
        if not dataset:
            raise Exception("No dataset provided")
        if 'sandbox' in kwargs:
            return shell.api('%s/%s/%s' % (BASE_PATH, dataset, kwargs['sandbox']), None)
        return shell.api('%s/%s' % (BASE_PATH, dataset), None)

    def modify(self, shell, **kwargs):
        if ((kwargs.get("action") != 'remove_path') and
            (kwargs.get('new_value', None) == None)):
            print("No new_value is given")
            raise Exception('new_value value missing')
        kwargs['type'] = self.ds_type
        kwargs['suffix'] = kwargs.pop("action", '')
        if ('dataset' not in kwargs and not shell._dataset) or ('dataset' in kwargs and not kwargs['dataset']):
            return "Dataset is not provided"
        url = self._get_base_url(shell,  **kwargs)
        return shell.api(url, {"new_value": kwargs.get('new_value', None),
                               "path": kwargs.get('path'), "save_on_error": kwargs.get('save_on_error', False)})

    def purge(self, shell, **kwargs):
        base_url = self._get_base_url(shell, **kwargs)
        if shell.confirm("Requesting the dataset[%s] to be purged. "
                   "Configuration will be lost for %s in %s." % (
                    kwargs.get('dataset'), shell._tenant, shell.server), kwargs):
            kwargs['action'] = 'purge'
            url = '{}&{}'.format(base_url, urlencode(kwargs))
            return shell.api(url, {})
        else:
            print("Purge not confirmed")
            return

    def create(self, shell, **kwargs):
        url = self._get_base_url(shell, **kwargs)
        url = '{}&{}'.format(url, urlencode({'save_on_error': kwargs.pop('save_on_error', False)}))
        return shell.api(url, kwargs.get('new_value', None))

    def summary(self, shell, **kwargs):
        ds = self.getinfo(shell, **kwargs)
        print("%s [%s]" % (ds['name'], ds.get('ds_type', None)))
        for attrset in ds.keys():
            if attrset == 'name' or attrset == 'ds_type':
                continue
            print("  +  %s" % attrset)
            for attrname, attrval in ds[attrset].items():
                if(isinstance(attrval, dict)):
                    valtype = attrval.get('type')
                else:
                    valtype = None
                if(valtype):
                    print("  |    + %s [%s]" % (attrname, valtype))
                else:
                    print("  |    + %s" % attrname)

    def getinfo(self, shell, **kwargs):
        url = self._get_base_url(shell, **kwargs)
        full_config = kwargs.get("full_config", False)
        url += "&full_config=" + str(full_config)
        dataset_info = shell.api(url, None)

        req_ft = kwargs.get('file_type')
        if req_ft:
            return dataset_info.get('file_types', {}).get(req_ft)

        req_map = kwargs.get('map')
        if req_map:
            return dataset_info.get('maps', {}).get(req_map)

        req_model = kwargs.get('model')
        if req_model:
            return dataset_info.get('models', {}).get(req_model)

        req_param = kwargs.get('param')
        if req_param:
            return dataset_info.get('params', {}).get(req_param)

        req_field = kwargs.get('field')
        if req_field:
            return dataset_info.get('fields', {}).get(req_field)
        return dataset_info
