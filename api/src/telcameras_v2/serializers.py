from rest_framework import serializers

from .models import Observation, CountAggregate, PersonAggregate


class CountAggregateSerializer(serializers.ModelSerializer):
    class Meta:
        model = CountAggregate


class ObservationSerializer(serializers.ModelSerializer):
    message = serializers.IntegerField()
    message_type = serializers.CharField(allow_blank=True)
    version = serializers.CharField(allow_blank=True)
    aggregate = serializers.ListField()

    class Meta:
        model = Observation
        fields = [
            'sensor',
            'sensor_type',
            'sensor_state',
            'owner',
            'supplier',
            'purpose',
            'latitude',
            'longitude',
            'interval',
            'timestamp_message',
            'timestamp_start',
            'message',
            'message_type',
            'version',
            'aggregate'
        ]

    def create(self, validated_data):
        message = validated_data.pop('message')
        message_type = validated_data.pop('message_type')
        version = validated_data.pop('version')
        aggregates = validated_data.pop('aggregate')
        observation = Observation.objects.create(**validated_data)

        if message_type == 'count':
            for aggregate in aggregates:
                aggregate['external_id'] = aggregate.pop('id')
                count_aggr = CountAggregate(**aggregate)
                count_aggr.observation = observation
                count_aggr.message = message
                count_aggr.version = version
                count_aggr.save()

        # TODO: these need to be removed
        observation.message = message
        observation.message_type = message_type
        observation.version = version
        observation.aggregate = aggregates

        return observation
