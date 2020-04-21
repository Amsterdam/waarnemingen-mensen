from django.contrib.postgres.fields import JSONField
from django.db import models


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


class MeasurementDetail(models.Model):
    id = models.UUIDField(primary_key=True)
    peoplemeasurement = models.ForeignKey('PeopleMeasurement', on_delete=models.CASCADE)
    direction = models.CharField(max_length=255)  # Can be either one of up, down, density or speed
    count = models.FloatField()
