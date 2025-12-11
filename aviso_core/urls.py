"""
URL configuration for aviso-core project.
"""
from django.contrib import admin
from django.urls import path, include
from django.conf import settings
from django.conf.urls.static import static

# API versioning
urlpatterns = [
    path('admin/', admin.site.urls),
    
    # API endpoints with versioning
    path('api/v1/', include('api.v1.urls')),
    path('api/v2/', include('api.v2.urls')),
    
    # Backward compatibility - old endpoint redirects to v2
    path('api/drilldown-fields/v2/', include('api.v2.urls')),
    
    # Main API routing (for cleaner URLs)
    path('api/', include('api.urls')),
]

# Serve media files in development
if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
    urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)

