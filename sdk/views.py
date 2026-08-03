import json
import os

from django.conf import settings
from django.http import HttpResponse, JsonResponse
from django.views import View


class SDKVersion(View):
    """avisosdk reads the version from the SDK_VERSION response header.

    The middleware stamps that header on every response; the body is returned
    for symmetry with the platform's other services.
    """

    http_method_names = ['get']

    def get(self, request, *args, **kwargs):
        return JsonResponse({'version': settings.SDK_VERSION})


class GetLatestSDK(View):
    """Serve the package directly, used when the SDK runs with sdk_debug on."""

    http_method_names = ['get']

    def get(self, request, *args, **kwargs):
        archive = os.path.join(settings.STATIC_ROOT, '%s.zip' % settings.SDK_VERSION)
        if not os.path.exists(archive):
            return HttpResponse(json.dumps({'error': 'SDK package is not built'}),
                                status=404, content_type='application/json')
        with open(archive, 'rb') as bundle:
            response = HttpResponse(bundle.read(), content_type='application/zip')
        response['Content-Disposition'] = ('attachment; filename="%s.zip"'
                                           % settings.SDK_VERSION)
        return response
