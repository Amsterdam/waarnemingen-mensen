import logging

from django.db import transaction, connection
from django.core.management.base import BaseCommand

log = logging.getLogger(__name__)


VIEW_NAME = 'cmsa_15min_materialized'

@transaction.atomic
class Command(BaseCommand):
    def handle(self, *args, **options):
        log.info(f"Start refreshing the {VIEW_NAME}")
        with connection.cursor() as cursor:
            cursor.execute(f"REFRESH MATERIALIZED VIEW {VIEW_NAME};")
        log.info(f"Done refreshing the {VIEW_NAME}")
