import re

from django.core.management.base import BaseCommand

from ingress.models import Endpoint


class Command(BaseCommand):
    help = "Removes an existing ingress endpoint"

    def add_arguments(self, parser):
        parser.add_argument(
            'url_key',
            help="A short string which is shown in the endpoint. For example: with the url_key "
                 "being 'people_data_v2', the endpoint is '/ingress/people_data_v2' (with no slash at the end).")

    def handle(self, *args, **options):
        url_key = options['url_key']

        # Check whether it already exists
        try:
            endpoint = Endpoint.objects.filter(url_key=url_key).get()
        except Endpoint.DoesNotExist:
            self.stdout.write(f"The endpoint '{url_key}' doesn't exist yet. Nothing has been done.")
            return

        # Remove the endpoint
        result = endpoint.delete()
        if result[0] == 1:
            self.stdout.write(f"Successfully removed the endpoint '{url_key}'")
        else:
            self.stdout.write(f"FAILED to remove the endpoint with url_key '{url_key}'")
