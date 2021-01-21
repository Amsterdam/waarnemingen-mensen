from rest_framework import serializers

from telcameras_v3.models import GroupAggregate, Observation, Person
from telcameras_v3.tools import scramble_group_aggregate


class PersonSerializer(serializers.ModelSerializer):
    class Meta:
        model = Person
        fields = [
            'observation_timestamp',
            'record',
            'distance',
            'time',
            'speed',
            'person_observation_timestamp',
            'type',
        ]


class GroupAggregateSerializer(serializers.ModelSerializer):
    persons = PersonSerializer(many=True)

    class Meta:
        model = GroupAggregate
        fields = [
            'observation_timestamp',
            'azimuth',
            'count',
            'cumulative_distance',
            'cumulative_time',
            'median_speed',
            'persons',
        ]


class ObservationSerializer(serializers.ModelSerializer):
    groupaggregates = GroupAggregateSerializer(many=True)

    class Meta:
        model = Observation
        fields = [
            'message_id',
            'timestamp',
            'sensor',
            'sensor_type',
            'sensor_state',
            'latitude',
            'longitude',
            'interval',
            'density',
            'groupaggregates',
        ]

    def create(self, validated_data):
        group_aggregates = validated_data.pop('groupaggregates')

        observation_obj = Observation.objects.create(
            **validated_data,
        )

        for group_aggregate_src in group_aggregates:
            persons = group_aggregate_src.pop('persons')
            group_aggregate_obj = GroupAggregate.objects.create(
                observation=observation_obj,
                **group_aggregate_src
            )

            # For privacy reasons we also copy the count to a new field which is slightly
            # scrambled. This shall be used in certain views and endpoints.
            count_aggregate = scramble_group_aggregate(group_aggregate_obj)
            count_aggregate.save()

            for person_src in persons:
                Person.objects.create(
                    group_aggregate=group_aggregate_obj,
                    **person_src
                )

        return observation_obj
