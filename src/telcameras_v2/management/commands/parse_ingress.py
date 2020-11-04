import json
import logging
import sys
import traceback
from datetime import datetime

import pytz
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction

from ingress.models import IngressQueue
from telcameras_v2.data_conversions import data_to_observation
from telcameras_v2.serializers import ObservationSerializer

log = logging.getLogger(__name__)
timezone = pytz.timezone("UTC")

AUTHORIZATION_HEADER = {'HTTP_AUTHORIZATION': f"Token {settings.AUTHORIZATION_TOKEN}"}


def parse(n=10):
    succes_counter = 0
    with transaction.atomic():
        ingresses = IngressQueue.objects.filter(endpoint='telcameras_v2').filter(parse_started__isnull=True).order_by('created_at')[:n].select_for_update(skip_locked=True)
        for ingress in ingresses:
            if ingress.parse_started is not None:
                continue

            # Mark it as being in the parsing stage
            ingress.parse_started = datetime.utcnow()
            ingress.save()

            try:
                with transaction.atomic():
                    # A try/except within an atomic transaction is not possible
                    # For this reason we add another transaction within this try/except
                    # https://docs.djangoproject.com/en/3.1/topics/db/transactions/#controlling-transactions-explicitly

                    data = json.loads(json.loads(ingress.raw_data))['data']
                    observation = data_to_observation(data)

                    observation_serializer = ObservationSerializer(data=observation)
                    observation_serializer.is_valid(raise_exception=True)
                    obj = observation_serializer.save()
                    if obj.id:
                        # Mark it as finished succesfully
                        ingress.parse_succeeded = datetime.utcnow()
                        ingress.save()
                        succes_counter += 1
                    else:
                        # Mark it as failed
                        ingress.parse_failed = datetime.utcnow()
                        ingress.save()

            except Exception:
                # Mark it as failed and save some info on the problem
                ingress.parse_failed = datetime.utcnow()
                stacktrace_str = ''.join(traceback.format_exception(*sys.exc_info()))
                ingress.parse_fail_info = stacktrace_str
                ingress.save()

    return succes_counter


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
        parsed = parse(n)
        self.stdout.write(str(parsed))
