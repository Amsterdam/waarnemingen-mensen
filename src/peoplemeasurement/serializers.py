import logging

from rest_framework import serializers

from .models import Area, Line, Sensors, Servicelevel

log = logging.getLogger(__name__)


class Today15minAggregationSerializer(serializers.Serializer):
    sensor = serializers.CharField()
    timestamp_rounded = serializers.DateTimeField()
    total_count = serializers.IntegerField()
    count_down = serializers.IntegerField()
    count_up = serializers.IntegerField()
    density_avg = serializers.FloatField()
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


class ServicelevelSerializer(serializers.ModelSerializer):
    class Meta:
        model = Servicelevel
        fields = '__all__'


class GeoSerializer(serializers.ModelSerializer):
    sensor = serializers.SlugRelatedField(slug_field="objectnummer", queryset=Sensors.objects.all())

    def get_validation_errors(self, errors=None) -> list[str]:
        """
        Returns all error messages in a list
        """
        default_errors = errors or self.errors
        error_messages = []
        for field_name, field_errors in default_errors.items():
            if isinstance(field_errors, list):
                for error in field_errors:
                    error_messages.append(f"{field_name}: {error}")
            else:
                error_messages += self.get_validation_errors(errors=field_errors)

        return error_messages


class AreaSerializer(GeoSerializer):
    class Meta:
        model = Area
        fields = '__all__'


class LineSerializer(GeoSerializer):
    class Meta:
        model = Line
        fields = '__all__'


class SensorSerializer(serializers.ModelSerializer):
    areas = AreaSerializer(many=True, read_only=True)
    lines = LineSerializer(many=True, read_only=True)

    class Meta:
        model = Sensors
        fields = '__all__'
