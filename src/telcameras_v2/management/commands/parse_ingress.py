from django.core.management.base import BaseCommand

from telcameras_v2.ingress_parser import TelcameraParser


class Command(BaseCommand):
    help = "Loops over the ingress table and parses new messages."
    def add_arguments(self, parser):
        parser.add_argument(
            '--records_per_batch',
            dest='records_per_batch',
            type=int,
            default=10,
            help='Maximum number of records to parse per batch (default 10)')

    def handle(self, *args, **options):
        n = options['records_per_batch']
        parser = TelcameraParser()
        parsed_count, success_count = parser.parse_n(n)
        self.stdout.write(f"Parsed: {parsed_count} Success: {success_count}")
