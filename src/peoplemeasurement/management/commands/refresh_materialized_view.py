import logging

from django.core.management.base import BaseCommand
from django.db import connection, transaction

log = logging.getLogger(__name__)


@transaction.atomic
class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument("view_name", type=str)
        parser.add_argument("--concurrently", default=False, action="store_true")

    def handle(self, *args, **options):
        view_name = options["view_name"]
        concurrently = "CONCURRENTLY" if options["concurrently"] else ""

        log.info(f"Start refreshing the {view_name}")
        refresh_query = f"REFRESH MATERIALIZED VIEW {concurrently} {view_name};"
        with connection.cursor() as cursor:
            cursor.execute(refresh_query)
        log.info(f"Done refreshing the {view_name}")
