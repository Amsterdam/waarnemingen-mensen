"""IOTSignals URL Configuration.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/1.9/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  url(r'^$', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  url(r'^$', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.conf.urls import url, include
    2. Add a URL to urlpatterns:  url(r'^blog/', include('blog.urls'))

"""
from django.conf import settings
from django.conf.urls import url, include
from django.urls import path

from drf_yasg.views import get_schema_view
from drf_yasg import openapi

from rest_framework import routers
from rest_framework import permissions

from passage import views as passage_views
from peoplemeasurement import views as peoplemeasurement_views

from iotsignals.routers import IOTSignalsRouterRoot
from iotsignals.routers import IOTSignalsRouterVersion0

root_router = IOTSignalsRouterRoot()

router_v0 = IOTSignalsRouterVersion0()

router_v0.register(
     r'milieuzone/passage',
     viewset=passage_views.PassageViewSet, base_name='passage')
router_v0.register(
     r'people/measurement',
     viewset=peoplemeasurement_views.PeopleMeasurementViewSet, base_name='peoplemeasurement')

urls = root_router.urls


schema_view = get_schema_view(
    openapi.Info(
        title="IOT Signals API",
        default_version='v1',
        description="IOTSignals in Amsterdam",
        terms_of_service="https://data.amsterdam.nl/",
        contact=openapi.Contact(email="datapunt@amsterdam.nl"),
        license=openapi.License(name="CC0 1.0 Universal"),
    ),
    validators=['flex', 'ssv'],
    public=True,
    permission_classes=(permissions.AllowAny,),
)


# urlpatterns = [
#     # url(r"^afval/stats/", include(stats.urls)),
#     url(r'^iotsignals/swagger(?P<format>\.json|\.yaml)$',
#         schema_view.without_ui(cache_timeout=None), name='schema-json'),
#     url(r'^iotsignals/swagger/$',
#         schema_view.with_ui('swagger', cache_timeout=None),
#         name='schema-swagger-ui'),
#     url(r'^iotsignals/redoc/$',
#         schema_view.with_ui('redoc', cache_timeout=None), name='schema-redoc'),
#
# ]

urlpatterns = [
    # API listings
    path('', include((root_router.urls, 'iotsignals'), namespace='vx')),
    # API Version 0
    path('v0/', include((router_v0.urls, 'iotsignals'), namespace='v0')),
    url(r"^status/", include("health.urls")),
]


if settings.DEBUG:
    import debug_toolbar

    urlpatterns.extend([
        url(r'^__debug__/', include(debug_toolbar.urls)),
    ])
