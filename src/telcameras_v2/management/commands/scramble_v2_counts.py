from django.core.management.base import BaseCommand
from django.db import transaction

from telcameras_v2.models import CountAggregate
from telcameras_v2.tools import scramble_count_aggregate


@transaction.atomic
def scramble_n_counts(n):
    records = CountAggregate.objects.filter(count_in__isnull=False, count_in_scrambled__isnull=True)[:n]

    count = records.count()
    if count == 0:
        return 0

    save_list = []
    for record in records:
        save_list.append(scramble_count_aggregate(record))

    CountAggregate.objects.bulk_update(records, ['count_in_scrambled', 'count_out_scrambled', 'count_scrambled'])

    return count


class Command(BaseCommand):
    help = "Adds either -1, 0 or 1 to count_in and count_out for privacy reasons."

    def handle(self, *args, **options):
        num_scrambled = None
        while num_scrambled != 0:
            num_scrambled = scramble_n_counts(10000)
