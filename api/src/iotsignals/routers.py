"""Router configuration for multiple versions"""

from django.urls import reverse
from rest_framework import routers

from iotsignals import API_VERSIONS
from iotsignals.version import get_version


class IOTSignalsAPIRootView(routers.APIRootView):
    """
    List IOT Signals API's and their related information.

    These API endpoints are part of the
    IOT Signalen Informatievoorziening Amsterdam

    The code for this application (and associated web front-end)
    is available from:

    - https://github.com/Amsterdam/iotsignals

    Note:
    Most of these endpoints (will) require some form of authentication.
    """

    def get(self, request, *args, **kwargs):
        response = super().get(request, *args, **kwargs)

        # Appending the index view with API version 0 information.
        v1root = request._request.build_absolute_uri(reverse('v0:api-root'))

        response.data['v0'] = {
            '_links': {
                'self': {
                    'href': v1root,
                }
            },
            'version': get_version(API_VERSIONS['v0']),
            'status': 'in development',
        }
        return response

    def get_view_name(self):
        return 'IOT Signals API'


class IOTSignalsAPIVersion0(routers.APIRootView):
    """Signalen API versie 0 (in development)."""

    def get_view_name(self):
        return 'Signals API Version 0'


class IOTSignalsRouterRoot(routers.DefaultRouter):
    APIRootView = IOTSignalsAPIRootView


class IOTSignalsRouterVersion0(routers.DefaultRouter):
    APIRootView = IOTSignalsAPIVersion0
