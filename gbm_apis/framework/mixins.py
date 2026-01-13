from django.conf import settings
from django.http import HttpResponse


class AvisoCompatibilityMixin:
    """
    Mixin to make legacy AvisoView compatible with standard Django 5 Middleware.

    Features:
    1. Shim Input: Adds 'request.params' (required by legacy baseView).
    2. Shim Output: Converts 'dict' responses to 'HttpResponse' (prevents Middleware crash).
    3. Dev Mode: Bypasses AvisoView's login check if settings.DEBUG is True.
    """

    def dispatch(self, request, *args, **kwargs):
        # --- 1. PATCH INPUT (Before View Execution) ---
        if not hasattr(request, 'params'):
            request.params = request.GET.copy()

        # --- 2. AUTH BYPASS (The "Loophole") ---
        # If we are local, disable the strict login check in the parent AvisoView
        if settings.DEBUG:
            self.login_required = False

        # --- 3. EXECUTE PARENT VIEW ---
        # This calls the next class in the chain (AvisoView or View)
        response = super().dispatch(request, *args, **kwargs)

        # --- 4. PATCH OUTPUT (After View Execution) ---
        # If legacy view returned a dict (e.g., auth error), convert it to HttpResponse
        if isinstance(response, dict):
            status = response.get('status', 200)
            body = response.get('body', '')
            headers = response.get('headers', {})

            content_type = headers.pop('Content-Type', 'application/json')
            django_resp = HttpResponse(content=body, status=status, content_type=content_type)

            for k, v in headers.items():
                django_resp[k] = v
            return django_resp

        return response