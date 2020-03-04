"""Router configuration for multiple versions"""

from django.urls import reverse
from rest_framework import routers

from settings import API_VERSIONS
from settings.version import get_version


class MensenAPIRootView(routers.APIRootView):
    """
    List IOT Signals API's and their related information.

    These API endpoints are part of the
    Waarnemingen Mensen Informatievoorziening Amsterdam

    The code for this application (and associated web front-end)
    is available from:

    - https://github.com/Amsterdam/waarnemingen-mensen
    """

    def get(self, request, *args, **kwargs):
        response = super().get(request, *args, **kwargs)

        # Appending the index view with API version 0 information.
        v1root = request._request.build_absolute_uri(reverse('v1:api-root'))

        response.data['v1'] = {
            '_links': {
                'self': {
                    'href': v1root,
                }
            },
            'version': get_version(API_VERSIONS['v1']),
            'status': 'in development',
        }
        return response

    def get_view_name(self):
        return 'Mensen API'


class MensenAPIVersion1(routers.APIRootView):
    """Mensen API versie 1."""

    def get_view_name(self):
        return 'Mensen API Version 1'


class MensenRouterRoot(routers.DefaultRouter):
    APIRootView = MensenAPIRootView


class MensenRouterVersion1(routers.DefaultRouter):
    APIRootView = MensenAPIVersion1
