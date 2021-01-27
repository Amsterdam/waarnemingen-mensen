from django.db import models
from django.utils import timezone

from contrib.timescale.fields import TimescaleDateTimeField


class Observation(models.Model):
    created_at = models.DateTimeField(auto_now_add=True)
    message_id = models.IntegerField()                      # Coming from "id" in the message. It's an auto-increment per sensor / volgnummer.
    timestamp = TimescaleDateTimeField(interval="1 day", default=timezone.now)   # Timestamp of when the data was recorded in the camera
    sensor = models.CharField(max_length=255)               # e.g. "CMSA-GAWW-17"
    sensor_type = models.CharField(max_length=255)          # type sensor (telcamera, wifi, bleutooth, 3d_camera etc)
    sensor_state = models.CharField(max_length=255)         # Coming from "status" e.g. "operational"
    latitude = models.DecimalField(max_digits=16, decimal_places=13)
    longitude = models.DecimalField(max_digits=16, decimal_places=13)
    interval = models.SmallIntegerField()                   # e.g. 60     # seconds that this message spans
    density = models.FloatField()


class GroupAggregate(models.Model):
    observation = models.ForeignKey('Observation', on_delete=models.CASCADE)
    created_at = models.DateTimeField(auto_now_add=True)
    observation_timestamp = TimescaleDateTimeField(interval="1 day", default=timezone.now)  # Copied from the observation
    azimuth = models.SmallIntegerField()
    count = models.SmallIntegerField()
    cumulative_distance = models.FloatField()
    cumulative_time = models.FloatField()
    median_speed = models.FloatField()


class Person(models.Model):
    group_aggregate = models.ForeignKey('GroupAggregate', on_delete=models.CASCADE)
    created_at = models.DateTimeField(auto_now_add=True)
    observation_timestamp = TimescaleDateTimeField(interval="1 day", default=timezone.now)  # Copied from the observation object
    record = models.UUIDField()
    distance = models.FloatField()
    time = models.FloatField()
    speed = models.FloatField()
    # NOTE! The person_observation_timestamp below is originally called "observation_timestamp" in the received json.
    # Unfortunately that overlaps with the observation_timestamp in this model which refers to the timestamp copied
    # from the observation model.
    person_observation_timestamp = models.DateTimeField()
    type = models.CharField(max_length=255)  # e.g. "pedestrian" or "cyclist" or maybe something else
