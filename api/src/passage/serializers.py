from rest_framework import serializers
# from rest_framework.response import Response

from datapunt_api.rest import HALSerializer
from datapunt_api.rest import DisplayField

from .models import Passage

import logging

log = logging.getLogger(__name__)


class PassageSerializer(HALSerializer):

    _display = DisplayField()

    class Meta:
        model = Passage
        fields = ['_display', '_links', 'id', 'versie', 'merk']


class PassageDetailSerializer(serializers.ModelSerializer):

    _display = DisplayField()

    class Meta:
        model = Passage
        fields = '__all__'
