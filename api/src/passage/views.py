from datapunt_api.rest import DatapuntViewSetWritable
from django_filters.rest_framework import DjangoFilterBackend
from django_filters.rest_framework import FilterSet


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
            'europese_voertuig_categorie',
            'europese_voertuig_categorie_toevoeging',
            'tax_indicator',
            'maximale_constructie_snelheid_bromsnorfiets',
        )


class PassageViewSet(DatapuntViewSetWritable):
    serializer_class = serializers.PassageDetailSerializer
    serializer_detail_class = serializers.PassageDetailSerializer

    queryset = models.Passage.objects.all().order_by('created_at')

    filter_backends = (DjangoFilterBackend,)
    filter_class = PassageFilter
