from django.core.management.base import BaseCommand

from ingress.tools import Endpoint


class Command(BaseCommand):
    help = "Enables the parser for an endpoint (if the parsers exist)."

    def add_arguments(self, parser):
        parser.add_argument(
            'url_key',
            help="The url_key for the endpoint. Use the 'list_endpoints' command to show the url_keys for "
                 "the existing endpoints.")

    def handle(self, *args, **options):
        url_key = options['url_key']

        endpoint = Endpoint.objects.get(url_key=url_key)
        if endpoint.parser_enabled:
            self.stdout.write(f"The parser for the endpoint '{url_key}' was already enabled. No changes were made.")
            return

        endpoint.parser_enabled = True
        endpoint.save()
        self.stdout.write(f"Enabled the parser for the endpoint with url_key '{url_key}'")
