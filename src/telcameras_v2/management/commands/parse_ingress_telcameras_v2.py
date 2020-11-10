from django.core.management.base import BaseCommand

from telcameras_v2.ingress_parser import TelcameraParser


class Command(BaseCommand):
    help = "Continuously loops over the ingress table and parses new messages."

    def handle(self, *args, **options):
        parser = TelcameraParser()
        parser.parse_continuously()
