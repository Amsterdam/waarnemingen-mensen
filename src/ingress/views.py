import logging
from typing import NamedTuple

from django.conf import settings
from django.http import HttpResponse
from django.shortcuts import get_object_or_404
from django.utils.decorators import method_decorator
from django.views import View
from django.views.decorators.csrf import csrf_exempt
from rest_framework.exceptions import AuthenticationFailed

from ingress.models import Endpoint, IngressQueue

logger = logging.getLogger(__name__)


class User(NamedTuple):
    is_authenticated: bool = False


@method_decorator(csrf_exempt, name='dispatch')
class IngressView(View):
    def post(self, request, queue):
        # Does the called endpoint exist
        endpoint = get_object_or_404(Endpoint, url_key=queue)
        if not endpoint.is_active:
            return HttpResponse("Endpoint is not active anymore", status=404, content_type='text/plain')

        # Is the user authorized to view this endpoint
        # TODO: replace this with the RBAC authentication
        if 'HTTP_AUTHORIZATION' not in request.META:
            return HttpResponse("Authorization header required.", status=401)
        if request.META['HTTP_AUTHORIZATION'].replace('Token ', '') != settings.AUTHORIZATION_TOKEN:
            return HttpResponse("Invalid token.", status=403)
        request.user = User(is_authenticated=True)

        # Save the data and send response
        raw_data = request.body.decode("utf-8")
        result = IngressQueue.objects.create(endpoint=endpoint, raw_data=raw_data)
        if result:
            return HttpResponse(status=200)

        logger.error(f"Message could not be saved: {raw_data}")
        return HttpResponse(status=500)
