import logging

from datapunt_api.rest import HALSerializer
from rest_framework import serializers

from .models import JsonDump

log = logging.getLogger(__name__)


class JsonDumpSerializer(HALSerializer):

    class Meta:
        model = JsonDump
        fields = [
            '_links',
            'id',
            'dump',
        ]


class JsonDumpDetailSerializer(serializers.ModelSerializer):

    class Meta:
        model = JsonDump
        fields = '__all__'
