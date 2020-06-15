from datapunt_api.pagination import HALCursorPagination
from datapunt_api.rest import DatapuntViewSetWritable
from django_filters.rest_framework import DjangoFilterBackend, FilterSet

from . import serializers
from .models import JsonDump


class JsonDumpViewSet(DatapuntViewSetWritable):
    serializer_class = serializers.JsonDumpSerializer
    serializer_detail_class = serializers.JsonDumpDetailSerializer

    queryset = JsonDump.objects.all()

    http_method_names = ['post']

    def get_serializer(self, *args, **kwargs):
        """
        All the incoming data has to be stored in one field (dump) in the db
        So let's take all the data from the root and put it in the 'dump' field"""
        request_body = kwargs.get("data")
        if request_body:
            new_request_body = {'dump': request_body}
            request_body = new_request_body
            kwargs["data"] = request_body

        serializer_class = self.get_serializer_class()
        kwargs['context'] = self.get_serializer_context()
        return serializer_class(*args, **kwargs)
