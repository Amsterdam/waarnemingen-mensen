import sys
import traceback
from abc import ABC, abstractmethod
from time import sleep

from django.db import transaction
from django.utils import timezone

from ingress.models import Endpoint, FailedIngressQueue, IngressQueue


class IngressParser(ABC):
    @property
    @abstractmethod
    def endpoint_url_key(self):
        pass

    @abstractmethod
    def parse_single_message(self, ingress):
        """ Implement parsing of one raw message. If it fails an exception has to be raised."""
        pass

    def handle_single_ingress_record(self, ingress):
        if ingress.parse_started is not None:
            return

        # Mark it as being in the parsing stage
        ingress.parse_started = timezone.now()
        ingress.save()

        try:
            # A try/except within an atomic transaction is not possible
            # For this reason we add another transaction within this try/except
            # https://docs.djangoproject.com/en/3.1/topics/db/transactions/#controlling-transactions-explicitly
            with transaction.atomic():
                self.parse_single_message(ingress.raw_data)
                ingress.parse_succeeded = timezone.now()
                ingress.save()

        except Exception:
            # In case of a parser fail we move the message to a separate failed ingress table
            failedingress = FailedIngressQueue()
            for field in ingress._meta.fields:
                if field.primary_key is not True:
                    setattr(failedingress, field.name, getattr(ingress, field.name))

            # Mark it as failed and save some info about the problem
            failedingress.parse_failed = timezone.now()
            stacktrace_str = ''.join(traceback.format_exception(*sys.exc_info()))
            failedingress.parse_fail_info = stacktrace_str
            failedingress.save()
            ingress.delete()

    def parse_continuously(self, end_at_empty_queue=False, end_at_disabled_parser=False, batch_size=100):
        try:
            endpoint = Endpoint.objects.get(url_key=self.endpoint_url_key)
        except Endpoint.DoesNotExist:
            print(f"\n    No endpoint exists with the url_key '{self.endpoint_url_key}'.")
            print("    Did you forget to create it? Run the command below to create it.")
            print(f"\n    python manage.py add_endpoint {self.endpoint_url_key}\n")
            return

        while True:
            endpoint.refresh_from_db()
            if not endpoint.parser_enabled:
                if end_at_disabled_parser:
                    break  # For testing purposes
                sleep(10)
                continue

            with transaction.atomic():
                # This locks N records and iterates over them. Parallel workers simply lock the next N records
                # Quote from https://www.postgresql.org/docs/11/sql-select.html :
                # "If a LIMIT is used, locking stops once enough rows have been returned to satisfy the limit"
                ingress_iterator = IngressQueue.objects.filter(endpoint=endpoint)\
                    .filter(parse_started__isnull=True) \
                    .order_by('created_at') \
                    .select_for_update(skip_locked=True)[:batch_size] \
                    .iterator()

                i = 0
                for ingress in ingress_iterator:
                    self.handle_single_ingress_record(ingress)
                    i += 1

                if i < batch_size:
                    if end_at_empty_queue:
                        break  # For testing purposes
                    sleep(1)
