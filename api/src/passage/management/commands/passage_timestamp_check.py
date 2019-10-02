from django.core.management.base import BaseCommand

from passage.models import Passage
from verify_timestamp import verify_timestamp


class Command(BaseCommand):
    def handle(self, *args, **options):
        latest = Passage.objects.order_by('created_at').last()

        if latest:
            verify_timestamp(latest.created_at, app='passage')
        else:
            raise Exception('Table is Empty')
