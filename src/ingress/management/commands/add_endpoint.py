from django.core.management.base import BaseCommand

from ingress.tools import add_endpoint


class Command(BaseCommand):
    help = "Adds a new ingress endpoint"
    def add_arguments(self, parser):
        parser.add_argument(
            'url_key',
            help="A short string which is shown in the endpoint. For example: with the url_key "
                 "being 'people_data_v2', the endpoint will be '/ingress/people_data_v2' (with no slash at the end).")

    def handle(self, *args, **options):
        url_key = options['url_key']
        endpoint_obj, message = add_endpoint(url_key)
        self.stdout.write(message)
