"""
URL configuration for aviso_core project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/5.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
import os
import logging

from django.contrib import admin
from django.urls import path, include, re_path


logger = logging.getLogger(f'gnana.{__name__}')

urlpatterns = [
    path('admin/', admin.site.urls),
    path('gbm/', include('gbm_apis.urls')),
]

aviso_apps = os.environ.get('AVISO_APPS', None)

if aviso_apps is not None:
    apps = aviso_apps.split(':')
    for app in apps:
        try:
            urlpatterns += [re_path(rf'^{app}/', include(f'{app}.urls'))]
        except Exception as e:
            logger.exception(f"Failed IMPORT!! while loading app: {app} urls ")
            raise