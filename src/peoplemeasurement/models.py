from django.contrib.gis.db import models


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
    details = models.JSONField(null=True)


class Sensors(models.Model):
    gid = models.IntegerField(unique=True)  # Used for reference in external applications
    geom = models.PointField(null=True)
    objectnummer = models.CharField(max_length=255, unique=True)
    soort = models.CharField(max_length=255, null=True, blank=True)
    voeding = models.CharField(max_length=255, null=True, blank=True)
    rotatie = models.IntegerField(null=True, blank=True)
    actief = models.CharField(max_length=255, null=True, blank=True)
    privacyverklaring = models.CharField(max_length=255, null=True, blank=True)
    location_name = models.CharField(max_length=255, null=True, blank=True)
    width = models.FloatField(null=True, blank=True)
    gebiedstype = models.CharField(max_length=255, null=True, blank=True)
    gebied = models.CharField(max_length=255, null=True, blank=True)
    imported_at = models.DateTimeField(null=True, auto_now_add=True)
    is_active = models.BooleanField(default=True)  # Can be used by the dashboard to display or hide sensors
    is_public = models.BooleanField(default=True)  # Defines whether the record can be displayed in publications

    def __str__(self):
        return self.objectnummer


class Servicelevel(models.Model):
    type_parameter = models.CharField(max_length=50)
    type_gebied = models.CharField(max_length=50)
    type_tijd = models.CharField(max_length=50)
    level_nr = models.IntegerField()
    level_label = models.CharField(max_length=50)
    lowerlimit = models.FloatField(blank=True, null=True)
    upperlimit = models.FloatField(blank=True, null=True)


class Area(models.Model):
    area_name = models.CharField(max_length=255, unique=True, null=True)  # Used for reference in external applications
    sensor = models.ForeignKey('Sensors', related_name='areas', on_delete=models.CASCADE)
    name = models.CharField(max_length=255, unique=True)  # Naam van meetgebied
    geom = models.PolygonField(blank=True)  # Polygoon dat het meetgebied omvat
    area = models.IntegerField()  # Oppervlakte van het meetgebied in m2

    class Meta:
        unique_together = ('sensor', 'name',)

    def __str__(self):
        return f"{self.name} ({self.sensor})"


class Line(models.Model):
    line_name = models.CharField(max_length=255, unique=True, null=True)  # Used for reference in external applications
    sensor = models.ForeignKey('Sensors', related_name='lines', on_delete=models.CASCADE)
    name = models.CharField(max_length=255, unique=True)  # Naam van de tellijn
    geom = models.LineStringField(blank=True)  # Lijn die de tellijn definieert
    azimuth = models.FloatField()  # Azimuth van de looprichting van de passage

    class Meta:
        unique_together = ('sensor', 'name',)

    def __str__(self):
        return f"{self.name} ({self.sensor})"

#   NOTE:
#   We added an extra table for this model called "peoplemeasurement_v1_data"
#   You can find it in the view_definitions script: https://github.com/Amsterdam/waarnemingen-mensen/blob/15832eb2027b90b78f6f387019dc7017f795619e/src/telcameras_v2/view_definitions.py#L4377-L4439
#
#   This creates an table which contains aggreated data from the v1 flow, 
#   based on the CMSA source code (see view_definitions). This can be 
#   done because the v1 data flow is outdated and therefore no new data 
#   is comming in. This gives us a huge performance boost when loading/
#   querying the cmsa data.
#   
#   Fields in this table are:
#   sensor              = Naam van de sensor
#   timestamp_rounded   = Tijdstip (per kwartier) waarop de telling van toepassing is
#   basedonxmessages    = Het aantal binnengekomen berichten (in betreffende kwartier) op basis waarvan deze telling tot stand is gekomen 
#   total_count         = Het toaal aantal tellingen van passanten 
#   count_down          = Het aantal getelde passanten in richting 1 (afhankelijk van de azimuth) 
#   count_up            = Het aantal getelde passanten in richting 2 (afhankelijk van de azimuth)
#   density_avg         = De dichtheid over het betreffende kwartier
#   speed_avg           = De gemiddelde snelheid over het betreffende kwartier
#
