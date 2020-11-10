from django.core.management.base import BaseCommand

from ingress.tools import NewEndpointError, add_endpoint


class Command(BaseCommand):
    help = "Adds a new ingress endpoint"
    def add_arguments(self, parser):
        parser.add_argument(
            'url_key',
            help="A short string which is shown in the endpoint. For example: with the url_key "
                 "being 'people_data_v2', the endpoint will be '/ingress/people_data_v2' (with no slash at the end).")

    def handle(self, *args, **options):
        url_key = options['url_key']
        
        try:
            add_endpoint(url_key)
            self.stdout.write(f"Created endpoint with url_key '{url_key}'")
        except NewEndpointError as e:
            self.stdout.write(str(e))
