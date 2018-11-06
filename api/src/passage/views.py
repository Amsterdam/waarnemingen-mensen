from datapunt_api.rest import DatapuntViewSetWritable
from django.contrib.gis.geos import Point
from django_filters.rest_framework import DjangoFilterBackend
from django_filters.rest_framework import FilterSet
from rest_framework.response import Response

from . import models
from . import serializers


class PassageFilter(FilterSet):

    class Meta(object):
        model = models.Passage
        fields = (
            'merk', 'voertuig_soort', 'versie', 'kenteken_land')


class PassageViewSet(DatapuntViewSetWritable):
    serializer_class = serializers.PassageReadOnlySerializer
    serializer_detail_class = serializers.PassageReadOnlySerializer

    queryset = models.Passage.objects.all()

    http_method_names = ['post', 'list', 'get']

    filter_backends = (DjangoFilterBackend,)
    filter_class = PassageFilter

    def create(self, request, *args, **kwargs):
        serializer = serializers.PassageWriteOnlySerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        serializer.save()
        return Response(serializer.data, status=201)
