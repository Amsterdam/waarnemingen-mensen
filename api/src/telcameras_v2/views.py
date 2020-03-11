from datapunt_api.rest import DatapuntViewSetWritable
from rest_framework import serializers

from .models import ObservationAggregate, PersonObservation, Sensor


class SensorSerializer(serializers.Serializer):

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

        sensors = Sensor.objects.filter(
            sensor_code=self.validated_data['sensor_code'],
            sensor_type=self.validated_data['sensor_type'],
            latitude=self.validated_data['latitude'],
            longitude=self.validated_data['longitude'],
            interval=self.validated_data['interval'],
            version=self.validated_data['version'],
        ).order_by('-id')
        if sensors.count() > 0:
            sensor = sensors[0]
        else:
            # The sensor doesn't exist exactly like posted, so we'll create a new one
            last_sensors = Sensor.objects.filter(sensor_code=self.validated_data['sensor_code']).order_by('-id')
            if last_sensors.count() > 0:
                # We've got a previous version of this sensor, so we'll clone it to inherit all that info
                sensor = last_sensors[0]
                sensor.pk = None  # this will create a new record
                sensor.sensor_type = self.validated_data['sensor_code']
                sensor.latitude = self.validated_data['latitude']
                sensor.longitude = self.validated_data['longitude']
                sensor.interval = self.validated_data['interval']
                sensor.version = self.validated_data['version']
                sensor.save()
            else:
                # There is no previous version of this sensor,
                # so we'll create one out of thin air
                # TODO: log this, so it doesn't go unnoticed
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
