import logging
from time import sleep
from django.conf import settings
from django.core.management.base import BaseCommand

from telcameras_v2.ingress_parser import TelcameraParser

log = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Continuously loops over the ingress table and parses new messages."

    def handle(self, *args, **options):
        if settings.ENABLED_PARSERS.get('parse_ingress_telcameras_v2') is not True:
            # The parser has not been activated explicitly
            # So we sleep and do nothing
            log.info("The mensen parser `parse_ingress_telcameras_v2` was deactivated and goes into sleep mode. Zzzz..")

            # Sleep forever (100 years)
            sleep(3153600000)

        parser = TelcameraParser()
        parser.parse_continuously()
