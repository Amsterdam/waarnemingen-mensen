from datapunt_api.rest import DatapuntViewSetWritable
from datapunt_api.pagination import HALCursorPagination
from django_filters.rest_framework import DjangoFilterBackend
from django_filters.rest_framework import FilterSet

from passage.case_converters import to_snakecase
from . import models
from . import serializers


class PassageFilter(FilterSet):

    class Meta(object):
        model = models.Passage
        fields = {
            'merk': ['exact'],
            'voertuig_soort': ['exact'],
            'indicatie_snelheid': ['exact'],
            'kenteken_nummer_betrouwbaarheid': ['exact'],
            'version': ['exact'],
            'kenteken_land': ['exact'],
            'toegestane_maximum_massa_voertuig': ['exact'],
            'europese_voertuigcategorie': ['exact'],
            'europese_voertuigcategorie_toevoeging': ['exact'],
            'taxi_indicator': ['exact'],
            'maximale_constructie_snelheid_bromsnorfiets': ['exact'],
            'created_at': ['exact', 'lt', 'gt'],
            'passage_at': ['exact', 'lt', 'gt'],
            'diesel': ['isnull', 'exact', 'lt', 'gt'],
            'gasoline': ['isnull', 'exact', 'lt', 'gt'],
            'electric': ['isnull', 'exact', 'lt', 'gt'],
        }


"""
from dateutil import tz

utcnow = datetime.datetime.utcnow().replace(tzinfo=tz.gettz('UTC'))
"""


class PassagePager(HALCursorPagination):
    """Sidcon pagination configuration.

    Fill-levels will be many. So we use cursor based pagination.
    """

    count_table = False
    page_size = 50
    max_page_size = 10000
    ordering = "-passage_at"


class PassageViewSet(DatapuntViewSetWritable):
    serializer_class = serializers.PassageDetailSerializer
    serializer_detail_class = serializers.PassageDetailSerializer

    queryset = models.Passage.objects.all().order_by('passage_at')

    http_method_names = ['post', 'list', 'get']

    filter_backends = (DjangoFilterBackend,)
    filter_class = PassageFilter

    pagination_class = PassagePager

    # override create to convert request.data from camelcase to snakecase.
    def create(self, request, *args, **kwargs):
        tmp = {to_snakecase(k): v for k, v in request.data.items()}
        request.data.clear()
        request.data.update(tmp)
        return super().create(request, *args, **kwargs)
