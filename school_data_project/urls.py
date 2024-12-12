"""
URL configuration for school_data_project project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/5.1/topics/http/urls/
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

from django.contrib import admin
from django.urls import path, include
from __data_processor__ import views

urlpatterns = [
    path("admin/", admin.site.urls),
]

urlpatterns += [
   
    path('data_processor/', include('__data_processor__.urls')),
]

urlpatterns += [
    path('', views.home, name='home'),  ## This is the default view
]
