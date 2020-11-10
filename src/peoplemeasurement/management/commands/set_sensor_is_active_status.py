from distutils.util import strtobool

from django.core.management.base import BaseCommand

from peoplemeasurement.models import Sensors


class Command(BaseCommand):
    help = "Activate or deactive a sensor"

    def add_arguments(self, parser):
        parser.add_argument(
            'objectnummer',
            help="A short string which represents the sensor. An example is 'GAWW-03'.")
        parser.add_argument('is_active', choices=['true', 'True', 'false', 'False'])

    def handle(self, *args, **options):
        objectnummer = options['objectnummer']
        is_active = bool(strtobool(options['is_active']))

        try:
            sensor = Sensors.objects.get(objectnummer=objectnummer)
        except Sensors.DoesNotExist:
            self.stdout.write(f"No sensor exists for the objectnummer '{objectnummer}'")
            return

        if is_active == sensor.is_active:
            self.stdout.write(f"The sensor '{objectnummer}'.is_active is already {is_active}. Nothing has changed.")
            return

        sensor.is_active = is_active
        sensor.save()
        self.stdout.write(f"The sensor '{objectnummer}'.is_active was successfully changed to {is_active}.")
