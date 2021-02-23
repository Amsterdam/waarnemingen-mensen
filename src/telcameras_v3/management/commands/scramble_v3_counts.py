from django.core.management.base import BaseCommand
from django.db import transaction

from telcameras_v3.models import GroupAggregate
from telcameras_v3.tools import scramble_group_aggregate


@transaction.atomic
def scramble_n_counts(n):
    records = GroupAggregate.objects.filter(count__isnull=False, count_scrambled__isnull=True)[:n]

    count = records.count()
    if count == 0:
        return 0

    save_list = []
    for record in records:
        save_list.append(scramble_group_aggregate(record))

    GroupAggregate.objects.bulk_update(save_list, ['count_scrambled'])

    return count


class Command(BaseCommand):
    help = "Adds either -1, 0 or 1 to count_in and count_out for privacy reasons."

    def handle(self, *args, **options):
        scrambled = None
        while scrambled != 0:
            scrambled = scramble_n_counts(10000)
