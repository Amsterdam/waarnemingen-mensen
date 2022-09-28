import logging
from datetime import date

from django.core.management.base import BaseCommand
from django.db import connection, transaction

from continuousaggregate.models import Cmsa15Min

log = logging.getLogger(__name__)


@transaction.atomic
class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument('--since', type=date.fromisoformat)

    def handle(self, *args, **options):
        since = options['since']

        since_log = ''
        if since:
            q = Cmsa15Min.objects.filter(timestamp_rounded__gt=since)
            since_log = since
        else:
            q = Cmsa15Min.objects.all()

        self.stdout.write(f"Start deleting records in aggregation table {Cmsa15Min._meta.db_table} since {since_log}")
        q.delete()
        self.stdout.write(f"Finished deleting records in aggregation table {Cmsa15Min._meta.db_table} since {since_log}")
