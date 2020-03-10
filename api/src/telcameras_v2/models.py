from django.db import models


class Sensor(models.Model):
    external_id = models.IntegerField()  # TODO: remove this?
    sensor_code = models.CharField(max_length=255)          # e.g. "CMSA-GAWW-17"
    sensor_type = models.CharField(max_length=255)          # type sensor (telcamera, wifi, bleutooth, 3d etc)
    latitude = models.FloatField()
    longitude = models.FloatField()
    interval = models.IntegerField()    # e.g. 60           # seconds that this message spans
    version = models.CharField(max_length=50)               # e.g. "2.0.0.2"        # versie van het bericht (zowel qua structuur als qua inhoud, dus mogelijk wijzigend met elke versiewijziging van de camerasoftware).

    # Extra info not in JSON
    owner = models.CharField(max_length=255, null=True)     # e.g. "gemeente Amsterdam" or  "Prorail"   # Verantwoordelijke Eigenaar van de Camera
    supplier = models.CharField(max_length=255, null=True)  # e.g. "HIG"            # Leverancier die de camera beheert in opdracht van de eigenaar.
    purpose = models.CharField(max_length=255, null=True)   # Doel van de camera
    area_gross = models.IntegerField(null=True)             # M2 in het zicht van de camera, inclusief bijv parkeerplaatsen en straatmeubilair.
    area_net = models.IntegerField(null=True)               # m2 dat bruikbaar is voor verkeer
    width = models.IntegerField(null=True)                  # Breedte meetvlak in m
    length = models.IntegerField(null=True)                 # Lengte meetvlak in m
    valid_from = models.DateTimeField(null=True)            # Datetime UTC Timestamp van moment waarop deze configuratie geldig werd.
    valid_until = models.DateTimeField(null=True)           # Timestamp van moment waarop de configuratie ongeldig werd.


class ObservationAggregate(models.Model):
    sensor = models.ForeignKey('Sensor', on_delete=models.CASCADE)
    aggregate_start = models.DateTimeField()                # The start of the measured time period  TODO: communicate this namechange from timestamp to start with Gerben

    # azimuth grouping in the json
    azimuth = models.IntegerField()
    count = models.IntegerField()
    cumulative_distance = models.FloatField()
    cumulative_time = models.IntegerField()
    median_speed = models.FloatField()


# TODO: make this a hypertable and make the UUIDfield a primary key (maybe together with the observation_timestamp)
class PersonObservation(models.Model):
    # Individual signal record
    record = models.UUIDField()
    sensor = models.ForeignKey('Sensor', on_delete=models.CASCADE)
    observation_aggregate = models.ForeignKey('ObservationAggregate', on_delete=models.CASCADE)
    observation_timestamp = models.DateTimeField()          # timestamp van de waarneming
    distance = models.FloatField(null=True)                 # gemeten afstand trajectory in meters
    time = models.FloatField(null=True)                     # gemeten tijd trajectory in seconden
    speed = models.FloatField(null=True)                    # snelheid passant in meter per seconden
    type = models.CharField(max_length=255, null=True)      # e.g. "pedestrian", and I guess it could also be "bike" etc


# class ColorThresholds(models.Model):
#     """ Used to create aggregate queries which can color the results """
#     sensor_code = models.CharField(max_length=255)          # e.g. "CMSA-GAWW-17"
#     color = models.CharField(max_length=255)                # e.g. RED, GREEN
#     threshold = models.FloatField()     # Drukte threshold waarboven de kleur rood/oranje/groen moet worden.
