import factory
from factory import fuzzy
from django.utils import timezone
from peoplemeasurement.models import PeopleMeasurement


class PeopleMeasurementFactory(factory.DjangoModelFactory):
    class Meta:
        model = PeopleMeasurement

    id = factory.Faker('uuid4')
    version = 't_version'
    timestamp = timezone.now()
    sensor = 't_sensor'
    sensortype = 't_sensor'
    latitude = fuzzy.FuzzyInteger(1.0, 100.0)
    longitude = fuzzy.FuzzyInteger(1.0, 100.0)
