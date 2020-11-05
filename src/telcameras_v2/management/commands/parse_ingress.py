import json

from django.core.management.base import BaseCommand

from ingress.parser import IngressParser
from telcameras_v2.data_conversions import data_to_observation
from telcameras_v2.serializers import ObservationSerializer


class TelcameraParser(IngressParser):
    endpoint_url_key = 'telcameras_v2'

    def parse_single_message(self, ingress_raw_data):
        data = json.loads(json.loads(ingress_raw_data))['data']
        observation = data_to_observation(data)

        observation_serializer = ObservationSerializer(data=observation)
        observation_serializer.is_valid(raise_exception=True)
        return observation_serializer.save()


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
