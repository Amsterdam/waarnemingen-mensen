from django.conf.urls import include, url
from rest_framework import routers

from . import views

router = routers.DefaultRouter()
router.register('', views.JsonDumpViewSet)

urlpatterns = router.urls
