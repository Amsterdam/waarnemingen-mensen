from django.core.management.base import BaseCommand

from ingress.tools import RedoFailedMessagesError, redo_failed_ingress_messages


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
            count = redo_failed_ingress_messages(url_key)
            end_message = f"\n\nMoved {count} messages from the failed queue to the normal queue to be " \
                          f"parsed again.\n\n"
            self.stdout.write(end_message)

        except RedoFailedMessagesError as e:
            self.stdout.write(str(e))
