from django.http import HttpResponse
from django.shortcuts import get_object_or_404
from django.views import View

from ingress.models import Endpoint, IngressQueue


class IngressView(View):
    def post(self, request, queue):
        if len(queue) == 0:
            return HttpResponse(status=404)

        endpoint = get_object_or_404(Endpoint, url_key=queue)

        result = IngressQueue.objects.create(endpoint=endpoint, raw_data=request.body.decode("utf-8"))
        if result:
            return HttpResponse(status=200)
        else:
            # TODO: improve error reporting. When does this actually need to fail?
            return HttpResponse(status=500)
