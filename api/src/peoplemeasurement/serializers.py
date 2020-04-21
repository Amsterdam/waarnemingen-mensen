import logging

from datapunt_api.rest import HALSerializer
from rest_framework import serializers

from .models import PeopleMeasurement, MeasurementDetail

log = logging.getLogger(__name__)


class PeopleMeasurementSerializer(HALSerializer):

    class Meta:
        model = PeopleMeasurement
        fields = [
            '_links',
            'id',
            'version',
            'timestamp',
            'sensor',
            'sensortype',
            'latitude',
            'longitude',
            'density',
            'speed',
            'count',
            'details',
        ]

    def create(self, validated_data):
        peoplemeasurement = PeopleMeasurement.objects.create(**validated_data)

        for detail in validated_data.get('details', []) or []:
            del detail['timestamp']  # The timestamp is always the same as the timestamp in the PeopleMeasurement object
            detail['peoplemeasurement_id'] = str(peoplemeasurement.id)
            MeasurementDetail.objects.create(**detail)
        
        peoplemeasurement.save()

        return peoplemeasurement


class PeopleMeasurementDetailSerializer(serializers.ModelSerializer):

    class Meta:
        model = PeopleMeasurement
        fields = '__all__'
