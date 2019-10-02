from django.core.management.base import BaseCommand

from peoplemeasurement.models import PeopleMeasurement
from verify_timestamp import verify_timestamp


class Command(BaseCommand):
    def handle(self, *args, **options):
        latest = PeopleMeasurement.objects.order_by('timestamp').last()

        if latest:
            verify_timestamp(latest.timestamp)
        else:
            raise Exception('Table is Empty')
