from rest_framework import serializers
from rest_framework.response import Response

from .models import Passage


class PassageSerializer(serializers.ModelSerializer):
    class Meta:
        model = Passage
        fields = ('versie',)

    def create(self, validated_data):
        print(validated_data)
