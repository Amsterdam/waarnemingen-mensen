from django.core.management.base import BaseCommand

from telcameras_v3.models import GroupAggregate
from telcameras_v3.tools import scramble_count


class Command(BaseCommand):
    help = "Adds either -1, 0 or 1 to count_in and count_out for privacy reasons."

    def handle(self, *args, **options):
        for record in GroupAggregate.objects.filter(count__isnull=False, count_scrambled__isnull=True):
            record = scramble_count(record)
            record.save()
