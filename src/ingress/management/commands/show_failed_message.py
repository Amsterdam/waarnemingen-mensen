from datetime import datetime

from django.core.management.base import BaseCommand

from ingress.models import Endpoint, FailedIngressQueue, IngressQueue


class ShowFailedMessagesError(Exception):
    pass


class Command(BaseCommand):
    help = "Show all info for the first failed record. Optionally an url_key can be defined to specify the endpoint."

    def add_arguments(self, parser):
        parser.add_argument(
            'url_key',
            nargs='?',
            default=None,
            help="The url_key for the endpoint. Use the 'list_endpoints' command to show the url_keys for "
                 "the existing endpoints."
            )

    def handle(self, *args, **options):
        url_key = options['url_key']
        if url_key:
            try:
                endpoint = Endpoint.objects.get(url_key=url_key)
            except Endpoint.DoesNotExist:
                raise ShowFailedMessagesError(
                    f"\n\nThe endpoint with url_key '{url_key}' does not exist. Nothing has been done.\n\n")

            failed_ingresses = FailedIngressQueue.objects.filter(endpoint=endpoint).order_by('created_at')
        else:
            failed_ingresses = FailedIngressQueue.objects.order_by('created_at')

        if failed_ingresses.count() == 0:
            raise ShowFailedMessagesError("No failed messages were found, nothing to show.")

        table_spacing = "{:<20} {:<50}"

        queue_attrs = ('created_at',
                       'parse_started',
                       'parse_succeeded',
                       'parse_failed',
                       'parse_fail_info',
                       'raw_data',
                       )

        failed_ingress = failed_ingresses.first()
        self.stdout.write("\n\n" + table_spacing.format('endpoint', failed_ingress.endpoint.url_key))
        for attr in queue_attrs:
            self.stdout.write(table_spacing.format(attr, str(getattr(failed_ingress, attr)) or 'None'))
