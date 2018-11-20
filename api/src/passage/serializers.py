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

    class Meta:
        model = Passage
        fields = '__all__'


data_keys_map = [
    ('straat', 'straat'),
    ('cameraId', 'camera_id'),
    ('cameraNaam', 'camera_naam'),
    ('cameraKijkrichting', 'camera_kijkrichting'),
    ('datumTijd', 'datum_tijd'),
    ('rijrichting', 'rijrichting'),
    ('rijstrook', 'rijstrook'),
]


class PassageWriteOnlySerializer(serializers.Serializer):
    """Passage save only serializer.

    We can optionaly get data from the data field.
    """

    id = serializers.UUIDField(required=True)
    type = serializers.CharField(required=True)
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

    # normalized field names.
    longitude = serializers.FloatField(required=False)
    latitude = serializers.FloatField(required=False)

    # added so filling in a form in drf is easy.
    straat = serializers.CharField(required=False)
    camera_id = serializers.CharField(required=False)
    camera_naam = serializers.CharField(required=False)
    camera_kijkrichting = serializers.CharField(required=False)
    datum_tijd = serializers.DateField(required=False)
    rijrichting = serializers.IntegerField(required=False)
    rijstrook = serializers.IntegerField(required=False)

    def validate(self, postdata):
        """Normalize input to new input.

        Allow specific fields to come from the nested data attribute.
        """
        normalized = dict(postdata)

        data = postdata.get('data', {})

        for key, field_key in data_keys_map.items():
            if getattr(field_key, data):
                continue
            if getattr(key, postdata):
                normalized[field_key] = data[key]
                continue

            raise serializers.ValidationError(
                'missing %s or %s', key, field_key)

        lon, lat = postdata.get('longitude'), postdata.get('latitude')

        if lon is None or lat is None:
            cameralocatie = data.get('cameraLocatie', {})
            if not cameralocatie:
                raise serializers.ValidationError('data.cameraLocatie missing')
            lon, lat = cameralocatie.get('coordinates', [0, 0])

        if not lon or not lat:
            raise serializers.ValidationError(
                'data.cameraLocatie or lon / lat are missing')

        camera_locatie = Point(lon, lat)
        normalized['camera_locatie'] = camera_locatie

        return normalized

    def save(self):

        Passage.objects.create(
            id=self.data['id'],
            versie=self.data['type'],
            straat=self.data['straat'],
            datum_tijd=self.data['datum_tijd'],
            rijrichting=self.data['rijrichting'],
            rijstrook=self.data['rijstrook'],

            camera_id=self.data['camera_id'],
            camera_naam=self.data['camera_naam'],
            camera_kijkrichting=self.data['camera_kijkrichting'],

            camera_locatie=self.data['camera_locatie'],

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
