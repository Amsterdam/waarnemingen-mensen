from django.db import models
from django.contrib.postgres.fields import JSONField
from django.contrib.gis.db.models import PointField
from django.core.validators import MaxValueValidator, MinValueValidator

from datetimeutc.fields import DateTimeUTCField


class Passage(models.Model):
    """Passage measurement.

    Each passing of a vehicle with a license plate passes into
    an environment zone which passes an environment camera
    should result in a record here.
    """

    id = models.UUIDField(primary_key=True, unique=True)
    passage_at = DateTimeUTCField(db_index=True, null=False)
    created_at = DateTimeUTCField(
        db_index=True, auto_now_add=True, editable=False)

    version = models.CharField(max_length=20)

    # camera properties
    straat = models.CharField(max_length=255)
    rijrichting = models.SmallIntegerField()
    rijstrook = models.SmallIntegerField()
    camera_id = models.CharField(max_length=255)
    camera_naam = models.CharField(max_length=255)
    camera_kijkrichting = models.FloatField()
    camera_locatie = PointField(srid=4326)

    # car properties
    kenteken_land = models.CharField(max_length=2)
    kenteken_nummer_betrouwbaarheid = models.SmallIntegerField(validators=[
        MaxValueValidator(1000),
        MinValueValidator(0)
    ])
    kenteken_land_betrouwbaarheid = models.SmallIntegerField(validators=[
        MaxValueValidator(1000),
        MinValueValidator(0)
    ])
    kenteken_karakters_betrouwbaarheid = JSONField(null=True)
    indicatie_snelheid = models.FloatField(null=True)
    automatisch_verwerkbaar = models.NullBooleanField()
    voertuig_soort = models.CharField(max_length=25, null=True)
    merk = models.CharField(max_length=255)
    inrichting = models.CharField(max_length=255)
    datum_eerste_toelating = models.DateField(null=True)
    datum_tenaamstelling = models.DateField(null=True)
    toegestane_maximum_massa_voertuig = models.SmallIntegerField(null=True)
    europese_voertuigcategorie = models.CharField(max_length=2)
    europese_voertuigcategorie_toevoeging = models.CharField(
        max_length=1, null=True)
    taxi_indicator = models.NullBooleanField()
    maximale_constructie_snelheid_bromsnorfiets = models.SmallIntegerField(
        null=True
    )

    # fuel properties
    brandstoffen = JSONField(null=True)
    extra_data = JSONField(null=True)
    diesel = models.SmallIntegerField(null=True)
    gasoline = models.SmallIntegerField(null=True)
    electric = models.SmallIntegerField(null=True)

    # TNO Versit klasse.
    # Zie ook: https://www.tno.nl/media/2451/lowres_tno_versit.pdf
    versit_klasse = models.CharField(null=True, max_length=255)
