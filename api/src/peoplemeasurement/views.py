import logging
from datetime import date

from datapunt_api.pagination import HALCursorPagination
from datapunt_api.rest import DatapuntViewSetWritable
from django.db import connection
from django_filters.rest_framework import DjangoFilterBackend, FilterSet
from rest_framework import exceptions, mixins, viewsets
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response

from . import serializers
from .models import PeopleMeasurement
from .queries import get_today_15min_aggregation_sql

logger = logging.getLogger(__name__)


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

    http_method_names = ['post']
    permission_classes = [IsAuthenticated]

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

    def create(self, request, *args, **kwargs):
        try:
            response = super().create(request, *args, **kwargs)
            return response
        except (exceptions.ValidationError, KeyError, TypeError) as e:
            logger.error(e)
            raise e


class Today15minAggregationViewSet(mixins.ListModelMixin, viewsets.GenericViewSet):
    def dictfetchall(self, cursor):
        """Return all rows from a cursor as a dict"""
        columns = [col[0] for col in cursor.description]
        return [
            dict(zip(columns, row))
            for row in cursor.fetchall()
        ]

    def list(self, request, *args, **kwargs):
        with connection.cursor() as cursor:
            cursor.execute(get_today_15min_aggregation_sql(datestr=str(date.today())))
            queryset = self.dictfetchall(cursor)
        serializer = serializers.Today15minAggregationSerializer(queryset, many=True)
        return Response(serializer.data)
