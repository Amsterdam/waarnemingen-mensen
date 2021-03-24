import logging

from datapunt_api.rest import HALSerializer
from rest_framework import serializers

from .models import PeopleMeasurement

log = logging.getLogger(__name__)


class Today15minAggregationSerializer(serializers.Serializer):
    sensor = serializers.CharField()
    timestamp_rounded = serializers.DateTimeField()
    total_count = serializers.IntegerField()
    count_down = serializers.IntegerField()
    count_up = serializers.IntegerField()
    density_avg = serializers.FloatField()
    speed_avg = serializers.FloatField()
    basedonxmessages = serializers.IntegerField()
    total_count_p10 = serializers.IntegerField()
    total_count_p20 = serializers.IntegerField()
    total_count_p50 = serializers.IntegerField()
    total_count_p80 = serializers.IntegerField()
    total_count_p90 = serializers.IntegerField()
    count_down_p10 = serializers.IntegerField()
    count_down_p20 = serializers.IntegerField()
    count_down_p50 = serializers.IntegerField()
    count_down_p80 = serializers.IntegerField()
    count_down_p90 = serializers.IntegerField()
    count_up_p10 = serializers.IntegerField()
    count_up_p20 = serializers.IntegerField()
    count_up_p50 = serializers.IntegerField()
    count_up_p80 = serializers.IntegerField()
    count_up_p90 = serializers.IntegerField()
    density_avg_p20 = serializers.FloatField()
    density_avg_p50 = serializers.FloatField()
    density_avg_p80 = serializers.FloatField()
    speed_avg_p20 = serializers.FloatField()
    speed_avg_p50 = serializers.FloatField()
    speed_avg_p80 = serializers.FloatField()
