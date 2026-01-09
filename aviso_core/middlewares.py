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

        # C. Get Tenant Identity
        # Priority 1: Env Vars (Your fix for local dev)
        # Priority 2: Default fallback
        tenant_name = os.environ.get('TENANT_NAME', 'wiz_qa.io')

        # D. Official Initialization
        # This loads the real context so your APIs can query MongoDB
        tenant_holder.set_context(
            user_name=microservices_user,
            tenant_name=tenant_name,
            login_tenant_name=tenant_name,
            login_user_name=microservices_user,
            switch_type="tenant",
            csv_version_info={},
        )

        # ===========================================================
        # 2. VIEW EXECUTION
        # ===========================================================
        response = self.get_response(request)

        # ===========================================================
        # 3. RESPONSE PHASE: Compatibility Fix
        # ===========================================================

        # --- CRITICAL FIX: Convert 'dict' to 'HttpResponse' ---
        # AvisoView returns a dictionary (e.g. {'status': 401...}) on errors.
        # Standard Django middleware crashes if it sees a dict.
        # We intercept it here and convert it to a real object.
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

        return response

# import os
# import threading
# from aviso.framework import tenant_holder
# from aviso.settings import microservices_user
#
#
# class SecurityContextMiddleware:
#     def __init__(self, get_response):
#         self.get_response = get_response
#
#     def __call__(self, request):
#         # 1. Initialize thread_local if missing (Standard Framework Requirement)
#         if not hasattr(tenant_holder, "thread_local"):
#             tenant_holder.thread_local = threading.local()
#
#         # 2. Get Tenant Identity from Env
#         tenant_name = os.environ.get('TENANT_NAME', 'wiz_qa.io')
#
#         # 3. Official Initialization
#         # This creates a UserDetails object that will attempt to load
#         # and decrypt the real Tenant configuration from MongoDB.
#         tenant_holder.set_context(
#             user_name=microservices_user,
#             tenant_name=tenant_name,
#             login_tenant_name=tenant_name,
#             login_user_name=microservices_user,
#             switch_type="tenant",
#             csv_version_info={},
#         )
#
#         response = self.get_response(request)
#         return response