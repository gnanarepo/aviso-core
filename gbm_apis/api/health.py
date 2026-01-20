import logging
from django.http import JsonResponse
from django.views import View
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from django.db import connection
from django.conf import settings

logger = logging.getLogger(f'gnana.{__name__}')


@method_decorator(csrf_exempt, name='dispatch')
class HealthCheckView(View):
    def get(self, request):
        """
        Health check endpoint that verifies basic application health
        Returns 200 if healthy, 503 if unhealthy
        """
        try:
            health_status = {
                "status": "healthy",
                "service": "aviso-core",
                "version": getattr(settings, 'APP_VERSION', '1.0.0'),
                "checks": {}
            }

            try:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT 1")
                health_status["checks"]["database"] = "ok"
            except Exception as db_error:
                logger.warning(f"Database health check failed: {str(db_error)}")
                health_status["checks"]["database"] = "failed"
                health_status["status"] = "degraded"

            logger.info("Health check passed")
            status_code = 200 if health_status["status"] == "healthy" else 503

            return JsonResponse(health_status, status=status_code)

        except Exception as e:
            logger.error(f"Health check endpoint error: {str(e)}", exc_info=True)
            error_response = {
                "status": "unhealthy",
                "service": "aviso-core",
                "error": str(e)
            }
            return JsonResponse(error_response, status=503)