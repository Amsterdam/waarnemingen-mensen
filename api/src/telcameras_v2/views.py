from datapunt_api.rest import DatapuntViewSetWritable
from rest_framework import serializers

from .models import ObservationAggregate, PersonObservation, Sensor

# class PersonObservationSerializer(serializers.Serializer):
#     class Meta:
#         model = models.PersonObservation
#         fields = [
#             'record',
#             'observation_timestamp',
#             'distance',
#             'time',
#             'speed',
#             'type',
#         ]
#
#
# class ObservationAggregateSerializer(serializers.Serializer):
#     class Meta:
#         model = models.ObservationAggregate
#         fields = [
#             'aggregate_start',  # Must come from sensorserializer.timestamp
#             'azimuth',
#             'count',
#             'cumulative_distance',
#             'cumulative_time',
#             'median_speed',
#         ]


class SensorSerializer(serializers.Serializer):

    # TODO: get the existing sensor instead of writing a new one

    # observation_aggregates = ObservationAggregateSerializer(many=True)
    # person_observations = PersonObservationSerializer(many=True)

    class Meta:
        model = Sensor
        fields = [
            'external_id',
            'sensor_code',
            'sensor_type',
            'latitude',
            'longitude',
            'interval',
            'version',
            'direction',
        ]

    def to_internal_value(self, data):
        if data:
            data['external_id'] = data.pop('id')
            data['sensor_code'] = data.pop('sensor')

            for direction in data['direction']:
                direction['aggregate_start'] = data['timestamp']
            del data['timestamp']

        # data = super().to_internal_value(data)
        return data

    def save(self):
        directions = self.validated_data.pop('direction')
        try:
            sensor = Sensor.objects.get(external_id=self.validated_data['external_id'])
        except Sensor.DoesNotExist:
            sensor = Sensor.objects.create(**self.validated_data)

        for direction in directions:
            signals = direction.pop('signals')
            direction['sensor_id'] = sensor.id
            aggregate_obj = ObservationAggregate.objects.create(**direction)
            for signal in signals:
                signal['sensor_id'] = sensor.id
                signal['observation_aggregate_id'] = aggregate_obj.id
                PersonObservation.objects.create(**signal)

        return sensor


class RecordViewSet(DatapuntViewSetWritable):
    serializer_class = SensorSerializer
    serializer_detail_class = SensorSerializer

    http_method_names = ['post']
