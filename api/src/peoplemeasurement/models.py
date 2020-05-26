from django.contrib.gis.db import models
from django.contrib.postgres.fields import JSONField


class PeopleMeasurement(models.Model):
    """PeopleMeasurement

    This models describes data coming from various sensors, such as
    counting cameras, 3D cameras and wifi sensors. The information
    contains for example people counts, direction, speed, lat/long etc.
    """

    id = models.UUIDField(primary_key=True)
    version = models.CharField(max_length=10)
    timestamp = models.DateTimeField(db_index=True)
    sensor = models.CharField(max_length=255)
    sensortype = models.CharField(max_length=255)
    latitude = models.DecimalField(max_digits=14, decimal_places=11)
    longitude = models.DecimalField(max_digits=14, decimal_places=11)
    density = models.FloatField(null=True)
    speed = models.FloatField(null=True)
    count = models.IntegerField(null=True)
    details = JSONField(null=True)


class Sensors(models.Model):
    geom = models.PointField(null=True)
    objectnummer = models.CharField(max_length=255, null=True)
    soort = models.CharField(max_length=255, null=True)
    voeding = models.CharField(max_length=255, null=True)
    rotatie = models.IntegerField(null=True)
    actief = models.CharField(max_length=255, null=True)
    privacyverklaring = models.CharField(max_length=255, null=True)
    location_name = models.CharField(max_length=255, null=True)
    width = models.FloatField(null=True)
