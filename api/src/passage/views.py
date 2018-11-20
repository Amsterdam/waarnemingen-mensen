from datapunt_api.rest import DatapuntViewSetWritable
from django_filters.rest_framework import DjangoFilterBackend
from django_filters.rest_framework import FilterSet
from rest_framework.response import Response

from . import models
from . import serializers


class PassageFilter(FilterSet):

    class Meta(object):
        model = models.Passage
        fields = (
            'merk',
            'voertuig_soort',
            'indicatie_snelheid',
            'kenteken_nummer_betrouwbaarheid',
            'versie',
            'kenteken_land',
            'toegestane_maximum_massa_voertuig',
            'europese_voertuigcategorie',
            'europese_voertuigcategorie_toevoeging',
            'tax_indicator',
            'maximale_constructie_snelheid_bromsnorfiets',
        )


class PassageViewSet(DatapuntViewSetWritable):
    serializer_class = serializers.PassageReadOnlySerializer
    serializer_detail_class = serializers.PassageReadOnlySerializer

    queryset = models.Passage.objects.all().order_by('datum_tijd')

    http_method_names = ['post', 'list', 'get']

    filter_backends = (DjangoFilterBackend,)
    filter_class = PassageFilter

    def create(self, request, *args, **kwargs):
        serializer = serializers.PassageWriteOnlySerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        serializer.save()
        return Response(serializer.data, status=201)
