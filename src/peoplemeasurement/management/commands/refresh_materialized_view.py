import logging

from django.db import transaction, connection
from django.core.management.base import BaseCommand

log = logging.getLogger(__name__)


@transaction.atomic
class Command(BaseCommand):

    def add_arguments(self, parser):
        parser.add_argument('view_name', type=str)

    def handle(self, *args, **options):
        view_name = options['view_name']

        log.info(f"Start refreshing the {view_name}")
        with connection.cursor() as cursor:
            cursor.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view_name};")
        log.info(f"Done refreshing the {view_name}")
