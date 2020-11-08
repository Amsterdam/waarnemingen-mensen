from django.core.management.base import BaseCommand

from ingress.models import Endpoint


class Command(BaseCommand):
    help = "List all existing ingress endpoints"

    def handle(self, *args, **options):
        endpoints = Endpoint.objects.all()

        self.stdout.write(f"Current number of endpoints: {endpoints.count()}\n")
        for endpoint in endpoints:
            self.stdout.write(endpoint.url_key + "\n")
