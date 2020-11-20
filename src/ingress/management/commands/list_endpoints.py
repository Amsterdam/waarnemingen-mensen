from django.core.management.base import BaseCommand

from ingress.models import Endpoint


class Command(BaseCommand):
    help = "List all existing ingress endpoints"

    def handle(self, *args, **options):
        endpoints = Endpoint.objects.all()

        self.stdout.write(f"\nCurrent number of endpoints: {endpoints.count()}\n\n")
        if endpoints.count() == 0:
            return

        table_spacing = "{:<4} {:<20} {:<10} {:<10}"
        self.stdout.write(table_spacing.format('id', 'url_key', 'is_active', 'parser_enabled'))
        for endpoint in endpoints:
            self.stdout.write(
                table_spacing.format(
                    endpoint.id,
                    endpoint.url_key,
                    endpoint.is_active,
                    endpoint.parser_enabled
                )
            )
