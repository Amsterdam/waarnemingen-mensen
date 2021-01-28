from django.core.management.base import BaseCommand

from centralerekenapplicatie_v1.ingress_parser import MetricParser


class Command(BaseCommand):
    help = "Continuously loops over the ingress table and parses new messages."

    def handle(self, *args, **options):
        parser = MetricParser()
        parser.parse_continuously()
