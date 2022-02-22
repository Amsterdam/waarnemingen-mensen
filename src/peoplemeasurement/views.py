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
            cursor.execute("""
                SELECT sensor,
                    timestamp_rounded,
                    total_count,
                    count_down,
                    count_up,
                    density_avg,
                    basedonxmessages,
                    total_count_p10,
                    total_count_p20,
                    total_count_p50,
                    total_count_p80,
                    total_count_p90,
                    count_down_p10,
                    count_down_p20,
                    count_down_p50,
                    count_down_p80,
                    count_down_p90,
                    count_up_p10,
                    count_up_p20,
                    count_up_p50,
                    count_up_p80,
                    count_up_p90,
                    density_avg_p20,
                    density_avg_p50,
                    density_avg_p80
                FROM continuousaggregate_cmsa15min
                WHERE timestamp_rounded > (NOW() - '1 day'::INTERVAL);
                """)
            queryset = self.dictfetchall(cursor)
        serializer = serializers.Today15minAggregationSerializer(queryset, many=True)
        return Response(serializer.data)


class SensorsDataViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = Sensors.objects.all()
    serializer_class = SensorSerializer
    pagination_class = None


class ServicelevelDataViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = Servicelevel.objects.all()
    serializer_class = ServicelevelSerializer
    pagination_class = None
