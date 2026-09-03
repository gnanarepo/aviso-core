import json
import logging

from django.http import HttpResponseNotFound, HttpResponseBadRequest
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt

from aviso.settings import sec_context
from aviso.utils import is_true
from aviso.common.datasetdesigner import create_dataset, modify_dataset, purge_dataset
 
from gbm_apis.framework.baseView import AvisoView
from gbm_apis.framework.mixins import AvisoCompatibilityMixin
from gbm_apis.domainmodel.datameta import Dataset
from utils import GnanaError



logger = logging.getLogger('gnana.%s' % __name__)

@method_decorator(csrf_exempt, name='dispatch')
class DatasetUpdate(AvisoCompatibilityMixin, AvisoView):
    ''' Update the dataset
    Few restrictions:
        - Names and Types can't be changed once created.
        - Understands only 2 levels of parameters.

    '''
    http_method_names = ['post']
    restrict_to_roles = {AvisoView.Role.Gnacker}

    def post(self, request, dataset=None, sandbox=None,
             action=None, *args, **kwargs):
        ds_type = request.GET.get('ds_type', '').lower()
        posted_data = request.read()
        username = request.user.username
        stage = sandbox
        if posted_data:
            attrs = json.loads(posted_data)
            path = attrs.pop("path")
            new_value = attrs.pop("new_value")
            save_on_error = attrs.pop('save_on_error', False)
        else:
            return HttpResponseBadRequest("No data provided")
        if(not isinstance(attrs, dict)):
            return HttpResponseBadRequest("Post data must be a map")
        result =  modify_dataset(dataset, stage, path, new_value, action, ds_type = ds_type,
                       username  = username, save_on_error = save_on_error)
        return result


@method_decorator(csrf_exempt, name='dispatch')
class DatasetView(AvisoCompatibilityMixin, AvisoView):
    '''
    Allows the customization of the dataset information.

    NOTE: Use Extreme caution when using this, since it will override the
    entire dataset.  This is meant to be used for quick startup using
    templates.

    For ongoing changes use the additional API that allows individual
    modifications to the dataset.

    get:
        Returns a json representation of the dataset. The following
        GET requests can be made.

        */dataset/<dataset name>*    -- Get entire dataset for
        the specified data set name. If stage information is present,
        stage data is also returned.

        */dataset/<dataset name>/<stage name>* -- Get entire dataset
        for the specified data set name and the stage name. Any data
        in the staging area supersedes the same attributeset in the
        core dataset.

    If the requested data is not found, 404 is returned.

    post:
        Reads the data stream as json and tries to create/update dataset
        based on what's requested. The following post calls can be made.

        /dataset/<dataset name> -- Post data must be empty or it is
            ignored. Creates a new dataset with the specified dataset
            name.

            If the dataset already exists, the resulting action will result
            on an error.

        Read the dataset documentation for proper format of the data that
        can be posted.
    '''
    restrict_to_roles = {AvisoView.Role.Gnacker, AvisoView.Role.User}

    def get(self, request, dataset=None, sandbox=None, attributeset=None,
            attributename=None, *args, **kwargs):
        ds_type = request.GET.get('ds_type', '').lower()
        full_config = request.GET.get('full_config', False) == 'True'
        if sandbox is None:
            sandbox = self.request_sandbox

        # Is the request for a specific stage information?
        ds = Dataset.getByNameAndStage(dataset, sandbox, full_config=full_config)
        if not ds:
            return HttpResponseNotFound("Dataset not found")

        # The requested stage is not found
        # if stage and not ds.stage_used:
        #     return HttpResponseNotFound("Given stage not found")

        if ds_type and (ds.ds_type != ds_type):
            return HttpResponseNotFound('Incompatible dataset')

        attrs = ds.get_as_map()
        if attributename:
            ret_value = attrs.get(attributeset, {}).get(attributename)
            if ret_value:
                return ret_value
            else:
                return HttpResponseNotFound("%s/%s not found in dataset" % (
                    attributeset, attributename))
        else:
            logger.info(' ------------------- returning data set ...... ')
            return attrs

    def post(self, request, dataset=None, sandbox=None, attributeset=None,
             attributename=None, *args, **kwargs):
        action = request.GET.get('action', 'create').lower()
        ds_type = request.GET.get('ds_type', '').lower()
        save_on_error = is_true(request.GET.get('save_on_error', False))
        logger.info(' save_on_error  %s' % save_on_error)
        posted_data = request.read()
        if posted_data:
            attrs = json.loads(posted_data)
        else:
            attrs = {}

        ds = Dataset.getByName(dataset)
        if 'sandbox' in attrs:
            sandbox = attrs.pop('sandbox')

        if action == 'create':
            if ds:
                if action == 'create':
                    return HttpResponseBadRequest(
                        content='Dataset Exists. Cannot post into dataset without a sandbox and specific module')

            # Create the dataset
            try:
                ds = create_dataset(dataset, ds_type, attrs, save_on_error, sandbox=sandbox)
            except Exception as e:
                logger.exception(e)
                return HttpResponseNotFound(e)
            return True

        if action == 'purge':
            try:
                tdetails = sec_context.details
                if tdetails.get_flag('save_on_error', ds.name, {}):
                    tdetails.remove_flag('save_on_error', ds.name)
                tdetails.save()
                purge_dataset(ds, dataset, ds_type)
            except Exception as e:
                logger.exception(e)
                return HttpResponseNotFound(e)
            return True

        raise GnanaError("Unknown action requested")