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
    gid = models.IntegerField(null=True)  # Used for reference in external applications
    geom = models.PointField(null=True)
    objectnummer = models.CharField(max_length=255, null=True)
    soort = models.CharField(max_length=255, null=True)
    voeding = models.CharField(max_length=255, null=True)
    rotatie = models.IntegerField(null=True)
    actief = models.CharField(max_length=255, null=True)
    privacyverklaring = models.CharField(max_length=255, null=True)
    location_name = models.CharField(max_length=255, null=True)
    width = models.FloatField(null=True)
    gebiedstype = models.CharField(max_length=255, null=True)
    gebied = models.CharField(max_length=255, null=True)
    imported_at = models.DateTimeField(null=True, auto_now_add=True)
    is_active = models.BooleanField(default=True)  # To decide if the received data from this sensor should be stored


class Servicelevel(models.Model):
    type_parameter = models.CharField(max_length=50)
    type_gebied = models.CharField(max_length=50)
    type_tijd = models.CharField(max_length=50)
    level_nr = models.IntegerField()
    level_label = models.CharField(max_length=50)
    lowerlimit = models.FloatField(blank=True, null=True)
    upperlimit = models.FloatField(blank=True, null=True)


class VoorspelCoefficient(models.Model):
    sensor = models.CharField(max_length=255)
    bron_kwartier_volgnummer = models.IntegerField()
    toepassings_kwartier_volgnummer = models.IntegerField()
    coefficient_waarde = models.FloatField()


class VoorspelIntercept(models.Model):
    sensor = models.CharField(max_length=255)
    toepassings_kwartier_volgnummer = models.IntegerField()
    intercept_waarde = models.FloatField()


class Area(models.Model):
    sensor = models.ForeignKey('Sensors', on_delete=models.CASCADE)
    name = models.CharField(max_length=255)  # Naam van meetgebied
    geom = models.PolygonField()  # Polygoon dat het meetgebied omvat
    area = models.IntegerField()  # Oppervlakte van het meetgebied in m2


class Line(models.Model):
    sensor = models.ForeignKey('Sensors', on_delete=models.CASCADE)
    name = models.CharField(max_length=255)  # Naam van de tellijn
    geom = models.LineStringField()  # Lijn die de tellijn definieert
    azimuth = models.FloatField()  # Azimuth van de looprichting van de passage

"""
#class V1Data(models.Model):
    table: peoplemeasurement_v1_data

    This model is an aggreated table which contains materialized data 
    based on the CMSA source code (see view_definitions). This can be 
    done because the v1 data flow is outdated and therefore no new data 
    is comming in. This gives us a huge performance boost when loading/
    querying the cmsa data.
    
    sensor = models.CharField(max_length=255)                               # Naam van de sensor
    timestamp_rounded = models.DateTimeField(db_index=True)                 # Tijdstip (per kwartier) waarop de telling van toepassing is
    basedonxmessages = models.IntegerField()                                # Het aantal binnengekomen berichten (in betreffende kwartier) op basis waarvan deze telling tot stand is gekomen 
    total_count = models.IntegerField()                                     # Het toaal aantal tellingen van passanten 
    count_down = models.IntegerField()                                      # Het aantal getelde passanten in richting 1 (afhankelijk van de azimuth) 
    count_up = models.IntegerField()                                        # Het aantal getelde passanten in richting 2 (afhankelijk van de azimuth)
    density_avg = models.DecimalField(max_digits=14, decimal_places=11)     # De dichtheid over het betreffende kwartier
    speed_avg = models.DecimalField(max_digits=14, decimal_places=11)       # De gemiddelde snelheid over het betreffende kwartier
"""