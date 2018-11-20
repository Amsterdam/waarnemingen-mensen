from django.db import models
from django.contrib.postgres.fields import JSONField, ArrayField
from django.core.validators import MaxValueValidator, MinValueValidator


class Passage(models.Model):
    """Passage measurement.

    Each passing of a vehicle with a license plate passes into
    an environment zone which passes an environment camera
    should result in a record here.
    """
    id = models.UUIDField(primary_key=True, unique=True)
    created_at = models.DateTimeField(auto_now_add=True)
    versie = models.CharField(max_length=255)
    data = JSONField()
    #
    kenteken_land = models.CharField(max_length=2)
    kenteken_nummer_betrouwbaarheid = models.SmallIntegerField(validators=[
        MaxValueValidator(1000),
        MinValueValidator(1)
    ])
    kenteken_land_betrouwbaarheid = models.SmallIntegerField(validators=[
        MaxValueValidator(1000),
        MinValueValidator(1)
    ])
    kenteken_karakters_betrouwbaarheid = JSONField()
    indicatie_snelheid = models.FloatField()
    automatisch_verwerkbaar = models.BooleanField()
    voertuig_soort = models.CharField(max_length=25)
    merk = models.CharField(max_length=255)
    inrichting = models.CharField(max_length=255)
    datum_eerste_toelating = models.DateField()
    datum_tenaamstelling = models.DateField()
    toegestane_maximum_massa_voertuig = models.SmallIntegerField()
    europese_voertuig_categorie = models.CharField(max_length=2)
    europese_voertuig_categorie_toevoeging = models.CharField(max_length=1)
    tax_indicator = models.BooleanField()
    maximale_constructie_snelheid_bromsnorfiets = models.SmallIntegerField()
    brandstoffen = JSONField()

    # NOTE make it an unmanaged model?
    # bezine
    # diesel
    # electtrisch
