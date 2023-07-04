from rest_framework import serializers

from telcameras_v2.tools import scramble_count_aggregate

from .models import CountAggregate, Observation, PersonAggregate


class CountAggregateSerializer(serializers.ModelSerializer):
    class Meta:
        model = CountAggregate
        fields = [
            "message",
            "version",
            "external_id",
            "type",
            "azimuth",
            "count_in",
            "count_out",
            "area",
            "geom",
            "count",
        ]


class PersonAggregateSerializer(serializers.ModelSerializer):
    class Meta:
        model = PersonAggregate
        fields = [
            "message",
            "version",
            "person_id",
            "observation_timestamp",
            "record",
            "speed",
            "geom",
            "quality",
            "distances",
        ]


class ObservationSerializer(serializers.ModelSerializer):
    counts = CountAggregateSerializer(many=True)
    persons = PersonAggregateSerializer(many=True)

    class Meta:
        model = Observation
        fields = [
            "sensor",
            "sensor_type",
            "sensor_state",
            "owner",
            "supplier",
            "purpose",
            "latitude",
            "longitude",
            "interval",
            "timestamp_message",
            "timestamp_start",
            "counts",
            "persons",
        ]

    def create(self, validated_data):
        counts = validated_data.pop("counts")
        persons = validated_data.pop("persons")

        observation = Observation.objects.create(
            **validated_data,
        )

        for count in counts:
            count_aggregate = CountAggregate(
                observation_timestamp_start=observation.timestamp_start,
                observation=observation,
                **count,
            )

            # For privacy reasons we also copy the count_in and count_out to two new fields which
            # are slightly scrambled. These shall be used in certain views and endpoints.
            count_aggregate = scramble_count_aggregate(count_aggregate)
            count_aggregate.save()

        for person in persons:
            PersonAggregate.objects.create(
                observation_timestamp_start=observation.timestamp_start,
                observation=observation,
                **person,
            )

        return observation
