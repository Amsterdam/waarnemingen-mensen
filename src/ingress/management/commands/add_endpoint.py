import re

from django.core.management.base import BaseCommand

from ingress.models import Endpoint


class Command(BaseCommand):
    help = "Adds a new ingress endpoint"
    def add_arguments(self, parser):
        parser.add_argument(
            'url_key',
            help="A short string which is shown in the endpoint. For example: with the url_key "
                 "being 'people_data_v2', the endpoint will be '/ingress/people_data_v2' (with no slash at the end).")

    def handle(self, *args, **options):
        url_key = options['url_key']
        if len(url_key) > 255:
            self.stdout.write("The url_key is larger than 255 characters. Please choose a shorter url_key.")
            return

        # Check that it only consists of sane characters
        if not re.match("^[A-Za-z0-9_-]*$", url_key):
            self.stdout.write("The url_key can only contain numbers, letters, underscores and dashes.")
            return

        # Check whether it already exists
        if Endpoint.objects.filter(url_key=url_key).count() > 0:
            self.stdout.write(f"The endpoint '{url_key}' already exists")
            return

        # Create the endpoint
        endpoint_obj = Endpoint.objects.create(url_key=url_key)

        if endpoint_obj.id:
            self.stdout.write(f"Created endpoint with url_key '{url_key}'")
        else:
            self.stdout.write(f"FAILED to create the endpoint with url_key '{url_key}'")
