from datapunt_api.rest import DatapuntViewSetWritable
from django_filters.rest_framework import DjangoFilterBackend
from django_filters.rest_framework import FilterSet


from . import models
from . import serializers


class PassageFilter(FilterSet):

    class Meta(object):
        model = models.Passage
        fields = (
            'merk', 'voertuig_soort', 'versie', 'kenteken_land')


class PassageViewSet(DatapuntViewSetWritable):
    serializer_class = serializers.PassageDetailSerializer
    serializer_detail_class = serializers.PassageDetailSerializer

    queryset = models.Passage.objects.all()

    filter_backends = (DjangoFilterBackend,)
    filter_class = PassageFilter
