from django.core.management.base import BaseCommand

from telcameras_v2.models import CountAggregate
from telcameras_v2.tools import scramble_counts


class Command(BaseCommand):
    help = "Adds either -1, 0 or 1 to count_in and count_out for privacy reasons."

    def handle(self, *args, **options):
        for record in CountAggregate.objects.filter(count_in__isnull=False, count_in_scrambled__isnull=True):
            record = scramble_counts(record)
            record.save()
