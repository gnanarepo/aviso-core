"""
Custom middleware for aviso-core Django application.
"""
import logging
from django.utils.deprecation import MiddlewareMixin

logger = logging.getLogger(__name__)


class SecurityContextMiddleware(MiddlewareMixin):
    """
    Middleware to initialize and set security context for each request.
    This ensures sec_context is available throughout the request lifecycle.
    """
    
    def process_request(self, request):
        """
        Initialize security context for the request.
        """
        try:
            # Import here to avoid circular imports
            from aviso.settings import sec_context
            from django.conf import settings as django_settings
            
            # If sec_context is not set, try to initialize it
            if sec_context is None:
                # Try to initialize from config/securitycontext if available
                try:
                    from config.securitycontext import initialize_system
                    # Only initialize if we have the necessary configuration
                    # For production, this should be set up differently
                    django_settings.sec_context = initialize_system()
                except Exception as e:
                    logger.warning(f"Could not initialize security context: {e}")
                    # Set a placeholder to avoid repeated initialization attempts
                    django_settings.sec_context = None
            
            # Make sec_context available on the request object
            request.sec_context = django_settings.sec_context or sec_context
            
        except Exception as e:
            logger.exception(f"Error in SecurityContextMiddleware: {e}")
            request.sec_context = None
    
    def process_response(self, request, response):
        """
        Cleanup after request processing.
        """
        # Any cleanup needed here
        return response

