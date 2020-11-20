import logging
from time import sleep
from django.conf import settings
from django.core.management.base import BaseCommand

from telcameras_v2.ingress_parser import TelcameraParser

log = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Continuously loops over the ingress table and parses new messages."

    def handle(self, *args, **options):
        parser = TelcameraParser()
        parser.parse_continuously()
