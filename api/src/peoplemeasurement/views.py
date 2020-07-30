import logging
import sys

from datapunt_api.pagination import HALCursorPagination
from datapunt_api.rest import DatapuntViewSetWritable
from django.db import connection, transaction
from django_filters.rest_framework import DjangoFilterBackend, FilterSet
from rest_framework import exceptions, generics, mixins, status, viewsets
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from writers import CSVStream

from . import serializers
from .models import PeopleMeasurement

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


class PeopleMeasurementViewSet(mixins.CreateModelMixin, viewsets.GenericViewSet):
    serializer_class = serializers.PeopleMeasurementSerializer
    serializer_detail_class = serializers.PeopleMeasurementDetailSerializer

    queryset = PeopleMeasurement.objects.all().order_by('timestamp')

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
        except exceptions.ValidationError as e:
            logger.error(f"{e} in message: {request.data}")
            return Response(str(e), status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            error_message = sys.exc_info()[1].__repr__()
            logger.error(error_message)

            long_error_message = f"Got {error_message} in message: {request.data}"
            # Also return the error message for easier debugging on the sending side
            return Response(long_error_message, status=status.HTTP_400_BAD_REQUEST)

    @action(methods=['get'], permission_classes=[], detail=False)
    def export(self, request, *args, **kwargs):
        # 1. Get the iterator of the QuerySet
        cursor = connection.cursor()
        cursor.execute("""
            select
                    sensor,
                    ps.location_name as location,
                    time_bucket('1 hour', p.timestamp) as datetime,
                    sum(count) as value
            from
                    (
                    select
                            distinct on
                            (timestamp, sensor, count) "timestamp", sensor, sensortype, count, details
                    from
                            public.peoplemeasurement_peoplemeasurement) as p
            left join peoplemeasurement_sensors ps on
                    p.sensor = ps.objectnummer
            where
                    lower(ps.actief) = 'ja'
            group by
                    datetime,
                    sensor,
                    location
        """)

        # 2. Create the instance of our CSVStream class
        csv_stream = CSVStream()

        columns = [column[0] for column in cursor.description]

        # 3. Stream (download) the file
        response = csv_stream.export(
            "export",
            cursor,
            lambda x: x,
            header=columns,
        )

        return response
