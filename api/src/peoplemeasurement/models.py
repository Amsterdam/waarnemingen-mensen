from django.db import models
from django.contrib.postgres.fields import JSONField

from datetimeutc.fields import DateTimeUTCField


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


class AbstractCSV(models.Model):
    id = models.CharField(primary_key=True, max_length=30)
    camera = models.CharField(max_length=100)
    timestamp = models.DateTimeField()
    direction_index = models.CharField(max_length=30)
    direction_name = models.CharField(max_length=30)
    label = models.CharField(max_length=255)
    value = models.FloatField(max_length=255)
    processed = models.CharField(max_length=30)

    class Meta:
        abstract = True


class PeopleMeasurementCSV(AbstractCSV):
    """
    This model describes the data retrieved from the csv file report.
    """
    csv_name = models.CharField(max_length=255, null=True)
    class Meta:
        ordering = ('timestamp',)


class PeopleMeasurementCSVTemp(AbstractCSV):
    """
    Temporary model that is used for importing the csv.
    Only used in the import process as a middle table before importing
    to PeopleMeasurementCSV
    """
    class Meta:
        ordering = ('timestamp',)
