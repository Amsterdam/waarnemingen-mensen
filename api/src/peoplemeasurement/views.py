import json
from django_filters.rest_framework import FilterSet

from django_filters.rest_framework import DjangoFilterBackend
from datapunt_api.pagination import HALCursorPagination
from datapunt_api.rest import DatapuntViewSetWritable

from .models import PeopleMeasurement
from . import serializers


class PeopleMeasurementFilter(FilterSet):
    class Meta(object):
        model = PeopleMeasurement
        fields = {
            'version': ['exact'],
            'timestamp': ['exact', 'lt', 'gt'],
            'sensor': ['exact'],
            'sensortype': ['exact'],
            'latitude': ['exact', 'lt', 'gt'],
            'longitude': ['exact', 'lt', 'gt'],
            'count': ['exact', 'lt', 'gt']
        }


class PeopleMeasurementPager(HALCursorPagination):
    count_table = False
    page_size = 100
    max_page_size = 10000
    ordering = '-timestamp'


class PeopleMeasurementViewSet(DatapuntViewSetWritable):
    serializer_class = serializers.PeopleMeasurementSerializer
    serializer_detail_class = serializers.PeopleMeasurementDetailSerializer

    queryset = PeopleMeasurement.objects.all().order_by('timestamp')

    http_method_names = ['post', 'list', 'get']

    filter_backends = [DjangoFilterBackend]
    filter_class = PeopleMeasurementFilter

    pagination_class = PeopleMeasurementPager

    def get_serializer(self, *args, **kwargs):
        """ The incoming data is in the `data` subfield. So I take it from there and put
        those items in root to store it in the DB"""
        request_body = kwargs.get("data")
        if request_body:
            new_request_body = request_body.get("data", {})
            new_request_body["details"] = request_body.get("details", None)
            request_body = new_request_body
            kwargs["data"] = request_body

        serializer_class = self.get_serializer_class()
        kwargs['context'] = self.get_serializer_context()
        return serializer_class(*args, **kwargs)
