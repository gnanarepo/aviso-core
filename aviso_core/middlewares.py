import threading
import uuid
import logging
from django.conf import settings
from django.http import HttpResponse
from aviso.framework import tenant_holder, tracer

from accounts import context

logger = logging.getLogger('gnana.%s' % __name__)

# Reachable without an identity — this is how a caller obtains one. They still
# go through the rest of the middleware, because logging in touches Mongo and
# Postgres and those connections have to be released like any other request.
ANONYMOUS_PATHS = {
    "/csrfform",
    "/account/login",
    "/account/logout",
    "/account/validate_ip",
    "/loginswitchbypass",
    "/sdk/version",
    "/sdk/latest",
}


def _release(tenant_name):
    """Close whatever this thread opened while serving the request."""
    try:
        pg_conn = getattr(tenant_holder, "postgres_local_con", None)
        if pg_conn:
            try:
                pg_conn.close()
            except Exception as e:
                logger.info("Failed to close Postgres connection: %s", e)

        mongo_db = getattr(tenant_holder, "tenant_db", None)
        if mongo_db:
            try:
                mongo_db.client.close()
            except Exception as e:
                logger.error("Failed to close Mongo connection: %s", e)

        logger.info(f"Context and DB Conn cleanup completed for tenant: {tenant_name}")

    except Exception as e:
        logger.error("Failed to clean up tenant connections: %s", e)
    finally:
        tracer.set_trace(None)


class SecurityContextMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        if request.path.rstrip("/") == "/gbm/health":
            return self.get_response(request)
    
        # ===========================================================
        # 1. REQUEST PHASE: Initialize Context
        # ===========================================================

        # A. Initialize thread_local (Required by your framework)
        if not hasattr(tenant_holder, "thread_local"):
            ##TODO ContextVar
            tenant_holder.thread_local = threading.local()

        # B. Reset to ensure clean state
        try:
            tenant_holder.reset_context()
        except:
            pass

        # C. Set trace_id — propagate from caller or generate a new one
        incoming_trace = (
            request.headers.get("X-Trace-Id") or
            request.headers.get("X-Request-ID")
        )
        trace_id = incoming_trace if incoming_trace else str(uuid.uuid4())
        tracer.set_trace(trace_id)

        # Identity first, context second: a request that fails to authenticate
        # must not leave a tenant context behind on this thread.
        if request.path.rstrip("/") in ANONYMOUS_PATHS:
            tenant_name, auth_mode = None, "anonymous"
        else:
            tenant_name, auth_mode = context.resolve(request)
            if not tenant_name:
                logger.warning("Unauthorized access attempt to %s", request.path)
                # resolve() reaches Mongo before it decides, so this return has
                # to release the connections the same way a served request does.
                _release(tenant_name)
                return context.unauthorized()

        logger.info("Received request for %s with trace_id: %s auth_mode=%s",
                    request.path, trace_id, auth_mode)

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

        if isinstance(response, HttpResponse):
            response['X-Trace-Id'] = trace_id

        # =====================================================
        # CLEANUP PHASE
        # =====================================================
        def cleanup():
            _release(tenant_name)

        if getattr(response, "streaming", False):
            original_stream = response.streaming_content

            def wrapped_stream():
                try:
                    for chunk in original_stream:
                        yield chunk
                finally:
                    cleanup()

            response.streaming_content = wrapped_stream()
        else:
            cleanup()

        return response
