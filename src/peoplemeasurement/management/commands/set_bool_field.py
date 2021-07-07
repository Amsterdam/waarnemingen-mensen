from distutils.util import strtobool

from django.core.management.base import BaseCommand

from peoplemeasurement.models import Sensors


class Command(BaseCommand):
    help = "Set the bool fields is_active, drop_incoming_data or is_public to true or false"

    def add_arguments(self, parser):
        parser.add_argument(
            'objectnummer',
            help="A short string which represents the sensor. An example is 'GAWW-03'.")

        parser.add_argument(
            'field',
            help="The field to set.",
            choices=['is_active', 'drop_incoming_data', 'is_public'])

        parser.add_argument('value', choices=['true', 'True', 'false', 'False'])

    def handle(self, *args, **options):
        objectnummer = options['objectnummer']
        field = options['field']
        value = bool(strtobool(options['value']))

        try:
            sensor = Sensors.objects.get(objectnummer=objectnummer)
        except Sensors.DoesNotExist:
            self.stdout.write(f"No sensor exists for the objectnummer '{objectnummer}'")
            return

        if value is getattr(sensor, field):
            self.stdout.write(f"The sensor '{objectnummer}'.{field} is already {value}. Nothing has changed.")
            return

        setattr(sensor, field, value)
        sensor.save()
        self.stdout.write(f"The sensor '{objectnummer}'.{field} was successfully changed to {value}.")
