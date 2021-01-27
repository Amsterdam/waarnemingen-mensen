from django.db import models

from contrib.timescale.fields import TimescaleDateTimeField


class CRAMetric(models.Model):
    created_at = models.DateTimeField(auto_now_add=True)
    message_id = models.TextField()  # Coming from "id" in the message.
    type = models.CharField(max_length=255)  # Either areaMetrics or lineMetrics
    sensor = models.CharField(max_length=255)  # e.g. "CMSA-GAWW-17"
    timestamp = models.DateTimeField()  # TODO Convert to timescale field
    # timestamp = TimescaleDateTimeField(interval="1 day")
    original_id = models.CharField(max_length=255)
    admin_id = models.IntegerField()

    class Meta:
        abstract = True


class AreaMetric(CRAMetric):
    area = models.FloatField()
    count = models.IntegerField()
    density = models.FloatField()
    total_distance = models.FloatField()
    total_time = models.FloatField()
    speed = models.FloatField(null=True)


class LineMetric(CRAMetric):
    pass


class LineMetricCount(models.Model):
    line_metric = models.ForeignKey(LineMetric, on_delete=models.CASCADE)
    azimuth = models.FloatField()
    count = models.IntegerField()
