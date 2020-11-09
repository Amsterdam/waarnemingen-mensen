import logging

from django.http import HttpResponse
from django.shortcuts import get_object_or_404
from django.views import View

from ingress.models import Endpoint, IngressQueue

logger = logging.getLogger(__name__)


class IngressView(View):
    def post(self, request, queue):
        endpoint = get_object_or_404(Endpoint, url_key=queue)
        if not endpoint.is_active:
            return HttpResponse("Endpoint is not active anymore", status=404, content_type='text/plain')

        raw_data = request.body.decode("utf-8")
        result = IngressQueue.objects.create(endpoint=endpoint, raw_data=raw_data)
        if result:
            return HttpResponse(status=200)
        else:
            # TODO: improve error reporting. When does this actually need to fail?
            logger.error(f"Message could not be saved: {raw_data}")
            return HttpResponse(status=500)
