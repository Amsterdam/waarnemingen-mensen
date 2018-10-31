from django.conf.urls import url, include
from rest_framework.routers import DefaultRouter

from . import views


router = DefaultRouter()
router.register('passage', views.PassageViewSet)

urlpatterns = [
    url(r'milieu/', include(router.urls))
]
