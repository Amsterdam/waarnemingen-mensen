from rest_framework import serializers

from centralerekenapplicatie_v1.models import CraCount, CraModel


class CountSerializer(serializers.ModelSerializer):
    class Meta:
        model = CraCount
        fields = ['azimuth', 'count']


class CraSerializer(serializers.ModelSerializer):
    counts = CountSerializer(many=True)

    class Meta:
        model = CraModel
        fields = [
            'message_id',
            'type',
            'sensor',
            'timestamp',
            'original_id',
            'admin_id',
            'area',
            'density',
            'total_distance',
            'total_time',
            'speed',
            'counts'
        ]

    def create(self, validated_data):
        counts = validated_data.pop('counts')

        cra_obj = CraModel.objects.create(
            **validated_data,
        )

        for count_src in counts:
            CraCount.objects.create(
                cra_model=cra_obj,
                **count_src
            )

        return cra_obj
