import sys
import traceback
from abc import ABC, abstractmethod
from datetime import datetime
from time import sleep

from django.db import transaction

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

    def parse_continuously(self, n=10):
        while True:
            parse_count, success_count = self.parse_n(n)
            if parse_count == 0:
                sleep(1)

    def parse_n(self, n=10):
        endpoint = Endpoint.objects.filter(url_key=self.endpoint_url_key).get()

        parse_counter = 0
        success_counter = 0
        with transaction.atomic():
            ingresses = IngressQueue.objects.filter(endpoint=endpoint)\
                            .filter(parse_started__isnull=True)\
                            .order_by('created_at')[:n].select_for_update(skip_locked=True)

            for ingress in ingresses:
                if ingress.parse_started is not None:
                    continue

                # Mark it as being in the parsing stage
                ingress.parse_started = datetime.utcnow()
                ingress.save()

                parse_counter += 1

                try:
                    # A try/except within an atomic transaction is not possible
                    # For this reason we add another transaction within this try/except
                    # https://docs.djangoproject.com/en/3.1/topics/db/transactions/#controlling-transactions-explicitly
                    with transaction.atomic():
                        self.parse_single_message(ingress.raw_data)
                        ingress.parse_succeeded = datetime.utcnow()
                        ingress.save()
                        success_counter += 1

                except Exception as e:
                    # In case of a parser fail we move the message to a separate failed ingress table
                    failedingress = FailedIngressQueue()
                    for field in ingress._meta.fields:
                        if field.primary_key == True:
                            continue  # don't want to clone the PK
                        setattr(failedingress, field.name, getattr(ingress, field.name))

                    # Mark it as failed and save some info about the problem
                    failedingress.parse_failed = datetime.utcnow()
                    stacktrace_str = ''.join(traceback.format_exception(*sys.exc_info()))
                    failedingress.parse_fail_info = stacktrace_str
                    failedingress.save()
                    ingress.delete()

        return parse_counter, success_counter
