from django.views import View
from ingress.models import IngressQueue
from django.http import HttpResponse


class IngressView(View):
    def post(self, request, queue):

        assert len(queue) > 0, "The queue name is not set."
        result = IngressQueue.objects.create(endpoint=queue, raw_data=request.body.decode("utf-8"))
        if result:
            return HttpResponse(status=200)
        else:
            # TODO: improve error reporting. When does this actually need to fail?
            return HttpResponse(status=500)
