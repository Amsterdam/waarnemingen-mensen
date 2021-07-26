import logging

from datapunt_api.pagination import HALPagination
from django.db import connection
from rest_framework import mixins, viewsets
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response

from contrib.rest_framework.authentication import SimpleGetTokenAuthentication
from peoplemeasurement import serializers
from peoplemeasurement.models import Area, Line, Sensors, Servicelevel
from peoplemeasurement.serializers import (AreaSerializer, LineSerializer,
                                           SensorSerializer,
                                           ServicelevelSerializer)

logger = logging.getLogger(__name__)


class Today15minAggregationViewSet(mixins.ListModelMixin, viewsets.GenericViewSet):
    authentication_classes = [SimpleGetTokenAuthentication]
    permission_classes = [IsAuthenticated]
    pagination_class = HALPagination

    def dictfetchall(self, cursor):
        """Return all rows from a cursor as a dict"""
        columns = [col[0] for col in cursor.description]
        return [
            dict(zip(columns, row))
            for row in cursor.fetchall()
        ]

    def list(self, request, *args, **kwargs):
        with connection.cursor() as cursor:
            cursor.execute("SELECT * FROM cmsa_15min_view_v10_realtime_predict;")
            queryset = self.dictfetchall(cursor)
        serializer = serializers.Today15minAggregationSerializer(queryset, many=True)
        return Response(serializer.data)


class SensorsDataViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = Sensors.objects.all()
    serializer_class = SensorSerializer


class ServicelevelDataViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = Servicelevel.objects.all()
    serializer_class = ServicelevelSerializer


class AreaDataViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = Area.objects.all()
    serializer_class = AreaSerializer


class LineDataViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = Line.objects.all()
    serializer_class = LineSerializer
