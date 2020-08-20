import logging

from django.db import transaction, connection
from django.core.management.base import BaseCommand

log = logging.getLogger(__name__)


@transaction.atomic
class Command(BaseCommand):
    def handle(self, *args, **options):
        log.info("Start refreshing the cmsa_15min_view_v3_materialized")
        with connection.cursor() as cursor:
            cursor.execute("REFRESH MATERIALIZED VIEW cmsa_15min_view_v3_materialized;")
        log.info("Done refreshing the cmsa_15min_view_v3_materialized")
