from rest_framework import viewsets

from . import models
from . import serializers


class PassageViewSet(viewsets.ModelViewSet):
    serializer_class = serializers.PassageSerializer
    queryset = models.Passage.objects.all()
