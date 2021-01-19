from django.db import models
from django.utils import timezone

from contrib.timescale.fields import TimescaleDateTimeField


class CraModel(models.Model):  # TODO: We need another name for this model
    created_at = models.DateTimeField(auto_now_add=True)
    message_id = models.TextField()                     # Coming from "id" in the message.
    type = models.CharField(max_length=255)             # Either areaMetrics or lineMetrics
    sensor = models.CharField(max_length=255)  # e.g. "CMSA-GAWW-17"
    timestamp = models.DateTimeField()  # TODO Convert to timescale field
    original_id = models.IntegerField()
    admin_id = models.IntegerField()

    # Fields that are only present in messages of type areaMetrics
    area = models.FloatField(null=True)
    density = models.FloatField(null=True)
    total_distance = models.FloatField(null=True)
    total_time = models.FloatField(null=True)
    speed = models.FloatField(null=True)


class CraCount(models.Model):
    cra_model = models.ForeignKey('CraModel', on_delete=models.CASCADE)
    azimuth = models.FloatField(null=True)
    count = models.IntegerField()
