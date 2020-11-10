from django.core.management.base import BaseCommand
from ingress.models import IngressQueue


class Command(BaseCommand):
    def handle(self, *args, **options):
        result = IngressQueue.objects.filter(parse_succeeded__isnull=False).delete()
        self.stdout.write(str(result[0]))
