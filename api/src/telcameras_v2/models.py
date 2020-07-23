from django.contrib.postgres import fields as postgres_fields
from django.db import models


class Observation(models.Model):
    sensor = models.CharField(max_length=255)               # e.g. "CMSA-GAWW-17"
    sensor_type = models.CharField(max_length=255)          # type sensor (telcamera, wifi, bleutooth, 3d_camera etc)
    sensor_state = models.CharField(max_length=255)         # e.g. "operational"
    owner = models.CharField(max_length=255, null=True)     # e.g. "gemeente Amsterdam" or  "Prorail"   # Verantwoordelijke Eigenaar van de Camera
    supplier = models.CharField(max_length=255, null=True)  # e.g. "Connection Systems"            # Leverancier die de camera beheert in opdracht van de eigenaar.
    purpose = postgres_fields.ArrayField(models.CharField(max_length=255), null=True)  # Doel van de camera
    latitude = models.DecimalField(max_digits=9, decimal_places=6)
    longitude = models.DecimalField(max_digits=9, decimal_places=6)
    interval = models.SmallIntegerField()                   # e.g. 60     # seconds that this message spans
    timestamp_message = models.DateTimeField()
    timestamp_start = models.DateTimeField()
    # message = models.IntegerField()                       # Volgnummer bericht. Is saved in the aggregates
    # message_type = models.CharField(max_length=50)        # either "count" OR "person". Is not stored, but used to determine which aggregate to use


class CountAggregate(models.Model):
    observation = models.ForeignKey('Observation', on_delete=models.CASCADE)
    message = models.IntegerField()                 # Volgnummer bericht, coming from root message
    version = models.CharField(max_length=50)       # e.g. "CS_count_0.0.1" versie van het bericht (zowel qua structuur als qua inhoud, dus mogelijk wijzigend met elke versiewijziging van de camerasoftware).

    external_id = models.CharField(max_length=255)  # Coming from the field "id". e.g. "Line 0" (no idea what else it can be).
    type = models.CharField(max_length=255)         # e.g. "line" or "zone". No idea what else it can be.

    # For a type "line"
    azimuth = models.PositiveSmallIntegerField(null=True)
    count_in = models.SmallIntegerField(null=True)
    count_out = models.SmallIntegerField(null=True)

    # for a type "zone"
    area = models.FloatField(null=True)
    geom = models.TextField(null=True)
    count = models.SmallIntegerField(null=True)


class PersonAggregate(models.Model):
    observation = models.ForeignKey('Observation', on_delete=models.CASCADE)
    message = models.IntegerField()                 # Coming from root message. Volgnummer bericht,
    version = models.CharField(max_length=50)       # Coming from root message. E.g. "CS_count_0.0.1" versie van het bericht (zowel qua structuur als qua inhoud, dus mogelijk wijzigend met elke versiewijziging van de camerasoftware).

    person_id = models.UUIDField()              # Coming from "personId"
    observation_timestamp = models.DateTimeField()
    record = models.IntegerField()
    speed = models.FloatField(null=True)
    geom = models.TextField(null=True)
    quality = models.IntegerField()
    distances = postgres_fields.JSONField()
