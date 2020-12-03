from django.core.management.base import BaseCommand
from django.db import transaction

from ingress.models import Endpoint, FailedIngressQueue, IngressQueue


class Command(BaseCommand):
    help = "Moves all failed messages for the specified endpoint back to the main ingress queue to be parsed again."

    def add_arguments(self, parser):
        parser.add_argument(
            'url_key',
            help="The url_key for the endpoint. Use the 'list_endpoints' command to show the url_keys for "
                 "the existing endpoints.")

    def handle(self, *args, **options):
        url_key = options['url_key']

        try:
            endpoint = Endpoint.objects.get(url_key=url_key)
        except Endpoint.DoesNotExist:
            self.stdout.write(f"\n\nThe endpoint with url_key '{url_key}' does not exist. Nothing has been done.\n\n")
            return

        moved_counter = 0
        with transaction.atomic():
            failed_ingresses = FailedIngressQueue.objects.filter(endpoint=endpoint).select_for_update(skip_locked=True)
            if failed_ingresses.count() == 0:
                self.stdout.write("No messages were found, so nothing was done.")
                return

            for failed_ingress in failed_ingresses:
                ingress = IngressQueue()
                ingress.created_at = failed_ingress.created_at
                ingress.endpoint = failed_ingress.endpoint
                ingress.raw_data = failed_ingress.raw_data
                ingress.save()
                failed_ingress.delete()
                moved_counter += 1

        end_message = f"\n\nMoved {moved_counter} messages from the failed queue to the normal queue to be parsed " \
                      f"again.\n\n"
        self.stdout.write(end_message)
