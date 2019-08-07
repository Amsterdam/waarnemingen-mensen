from django.contrib.gis.geos import Point

from rest_framework import serializers
from datapunt_api.rest import HALSerializer
from datapunt_api.rest import DisplayField

from .models import Passage

import logging
log = logging.getLogger(__name__)


class PassageSerializer(HALSerializer):

    _display = DisplayField()

    class Meta:
        model = Passage
        fields = [
            '_display',
            '_links',
            'id',
            'versie',
            'merk',
            'created_at',
            'passage_at',
        ]


class PassageDetailSerializer(serializers.ModelSerializer):
    id = serializers.UUIDField(validators=[])  # Disable the validators for the id, which improves performance (rps) by over 200%

    class Meta:
        model = Passage
        fields = '__all__'
