import os
import threading
import json
import uuid
import logging
from django.conf import settings
from django.http import HttpResponse
from aviso.framework import tenant_holder
from aviso.settings import microservices_user

logger = logging.getLogger(__name__)


class SecurityContextMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        # ===========================================================
        # 1. REQUEST PHASE: Initialize Context
        # ===========================================================

        # A. Initialize thread_local (Required by your framework)
        if not hasattr(tenant_holder, "thread_local"):
            tenant_holder.thread_local = threading.local()

        # B. Reset to ensure clean state
        try:
            tenant_holder.reset_context()
        except:
            pass

        tenant_name = (
                request.headers.get("X-Tenant-Name")
                or os.environ.get("TENANT_NAME", "wiz_qa.io")
        )

        if not tenant_name:
            if os.environ.get("CNAME").lower() == "app".lower():
                return HttpResponse(
                    json.dumps({"error": "Missing X-Tenant-Name"}),
                    status=400,
                    content_type="application/json",
                )
            tenant_name = os.environ.get("TENANT_NAME", "wiz_qa.io")

        tenant_holder.set_context(
            user_name=microservices_user,
            tenant_name=tenant_name,
            login_tenant_name=tenant_name,
            login_user_name=microservices_user,
            switch_type="tenant",
            csv_version_info={},
        )

        ##Internal API-Key Validation
        internal_api_key = request.headers.get("Internal-Api-Key")
        if not internal_api_key or internal_api_key != os.environ.get("INTERNAL_API_KEY", ""):
            logger.warning(f"Unauthorized access attempt to microservice by tenant: {tenant_name}")
            return HttpResponse(content=json.dumps({"error": "Unauthorized"}), status=401, content_type="application/json")
            
        response = self.get_response(request)

        if isinstance(response, dict):
            status_code = response.get('status', 200)
            body = response.get('body', '')
            headers = response.get('headers', {})

            # Default to JSON if not specified
            content_type = headers.pop('Content-Type', 'application/json')

            # Rebuild as standard Django Response
            new_response = HttpResponse(content=body, status=status_code, content_type=content_type)

            # Apply headers
            for k, v in headers.items():
                new_response[k] = v

            response = new_response

        # --- Legacy Headers (Optional but recommended) ---
        if isinstance(response, HttpResponse) and hasattr(settings, 'SDK_VERSION'):
            response['SDK_VERSION'] = settings.SDK_VERSION

        # =====================================================
        # CLEANUP PHASE
        # =====================================================
        try:
            tenant_db = getattr(tenant_holder, "tenant_db", None)
            if tenant_db:
                tenant_db.client.close()
                logger.info("Closed Mongo connection for tenant: %s", tenant_holder.name)
        except Exception as e:
            logger.warning("Failed to close Mongo connection: %s", e)

        return response
