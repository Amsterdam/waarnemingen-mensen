from rest_framework import serializers

from centralerekenapplicatie_v1.models import (AreaMetric, LineMetric,
                                               LineMetricCount)


class LineMetricCountSerializer(serializers.ModelSerializer):
    class Meta:
        model = LineMetricCount
        fields = ['line_metric_timestamp', 'azimuth', 'count']


class LineMetricSerializer(serializers.ModelSerializer):
    counts = LineMetricCountSerializer(many=True)

    class Meta:
        model = LineMetric
        fields = [
            'message_id',
            'sensor',
            'timestamp',
            'original_id',
            'admin_id',
            'counts'
        ]

    def create(self, validated_data):
        counts = validated_data.pop('counts')

        line_metric = LineMetric.objects.create(
            **validated_data,
        )

        for count_src in counts:
            LineMetricCount.objects.create(
                line_metric=line_metric,
                **count_src
            )

        return line_metric


class AreaMetricSerializer(serializers.ModelSerializer):
    class Meta:
        model = AreaMetric
        fields = [
            'message_id',
            'sensor',
            'timestamp',
            'original_id',
            'admin_id',
            'area',
            'count',
            'density',
            'total_distance',
            'total_time',
            'speed',
        ]
