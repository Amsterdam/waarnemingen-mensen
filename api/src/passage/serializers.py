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
        fields = ['_display', '_links', 'id', 'versie', 'merk']


class PassageReadOnlySerializer(serializers.ModelSerializer):

    class Meta:
        model = Passage
        fields = '__all__'


class PassageWriteOnlySerializer(serializers.Serializer):
    """Passage save only serializer.
    """

    type = serializers.CharField(required=True)
    id = serializers.UUIDField(required=True)
    data = serializers.DictField(required=True)
    kentekenLand = serializers.CharField(required=True)
    kentekenNummerBetrouwbaarheid = serializers.IntegerField(required=True)
    kentekenLandBetrouwbaarheid = serializers.IntegerField(required=True)
    kentekenKaraktersBetrouwbaarheid = serializers.ListField(required=True)
    indicatieSnelheid = serializers.FloatField(required=True)
    automatischVerwerkbaar = serializers.BooleanField(required=True)
    voertuigSoort = serializers.CharField(required=True)
    merk = serializers.CharField(required=True)
    inrichting = serializers.CharField(required=True)
    datumEersteToelating = serializers.DateField(required=True)
    datumTenaamstelling = serializers.DateField(required=True)
    toegestaneMaximumMassaVoertuig = serializers.IntegerField(required=True)
    europeseVoertuigcategorie = serializers.CharField(required=True)
    europeseVoertuigcategorieToevoeging = serializers.CharField(required=True)
    taxIndicator = serializers.BooleanField(required=True)
    maximaleContructiesnelheidBromSnorfiets = serializers.IntegerField(
        required=True
    )
    brandstoffen = serializers.ListField(required=True)

    def save(self):
        camera_locatie = Point(
            self.data['data']['cameraLocatie']['coordinates'][0],
            self.data['data']['cameraLocatie']['coordinates'][1]
        )
        Passage.objects.create(
            id=self.data['id'],
            versie=self.data['type'],
            datum_tijd=self.data['data']['datumTijd'],
            straat=self.data['data']['straat'],
            rijstrook=self.data['data']['rijstrook'],
            rijrichting=self.data['data']['rijrichting'],
            camera_id=self.data['data']['cameraId'],
            camera_naam=self.data['data']['cameraNaam'],
            camera_kijkrichting=self.data['data']['cameraKijkrichting'],
            camera_locatie=camera_locatie,
            kenteken_land=self.data['kentekenLand'],
            kenteken_nummer_betrouwbaarheid=self.data[
                'kentekenNummerBetrouwbaarheid'
            ],
            kenteken_land_betrouwbaarheid=self.data[
                'kentekenLandBetrouwbaarheid'
            ],
            kenteken_karakters_betrouwbaarheid=self.data[
                'kentekenKaraktersBetrouwbaarheid'
            ],
            indicatie_snelheid=self.data['indicatieSnelheid'],
            automatisch_verwerkbaar=self.data['automatischVerwerkbaar'],
            voertuig_soort=self.data['voertuigSoort'],
            merk=self.data['merk'],
            inrichting=self.data['inrichting'],
            datum_eerste_toelating=self.data['datumEersteToelating'],
            datum_tenaamstelling=self.data['datumTenaamstelling'],
            toegestane_maximum_massa_voertuig=self.data[
                'toegestaneMaximumMassaVoertuig'
            ],
            europese_voertuigcategorie=self.data[
                'europeseVoertuigcategorie'
            ],
            europese_voertuigcategorie_toevoeging=self.data[
                'europeseVoertuigcategorieToevoeging'
            ],
            tax_indicator=self.data['taxIndicator'],
            maximale_constructie_snelheid_bromsnorfiets=self.data[
                'maximaleContructiesnelheidBromSnorfiets'
            ],
            brandstoffen=self.data['brandstoffen']
        )
