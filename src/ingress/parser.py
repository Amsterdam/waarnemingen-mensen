import sys
import traceback
from abc import ABC, abstractmethod
from datetime import datetime

from django.db import transaction

from ingress.models import IngressQueue


class IngressParser(ABC):
    @property
    @abstractmethod
    def endpoint(self):
        pass

    @abstractmethod
    def parse_single_message(self, ingress):
        """ Implement parsing of one raw message and return an instance """
        pass

    def parse(self, n=10):
        success_counter = 0
        with transaction.atomic():
            ingresses = IngressQueue.objects.filter(endpoint=self.endpoint)\
                            .filter(parse_started__isnull=True)\
                            .order_by('created_at')[:n].select_for_update(skip_locked=True)

            for ingress in ingresses:
                if ingress.parse_started is not None:
                    continue

                # Mark it as being in the parsing stage
                ingress.parse_started = datetime.utcnow()
                ingress.save()

                try:
                    # A try/except within an atomic transaction is not possible
                    # For this reason we add another transaction within this try/except
                    # https://docs.djangoproject.com/en/3.1/topics/db/transactions/#controlling-transactions-explicitly

                    with transaction.atomic():
                        obj = self.parse_single_message(ingress.raw_data)
                        if obj.id:
                            # Mark it as finished successfully
                            ingress.parse_succeeded = datetime.utcnow()
                            ingress.save()
                            success_counter += 1
                        else:
                            # Mark it as failed
                            ingress.parse_failed = datetime.utcnow()
                            ingress.save()

                except Exception as e:

                    # Mark it as failed and save some info on the problem
                    ingress.parse_failed = datetime.utcnow()
                    stacktrace_str = ''.join(traceback.format_exception(*sys.exc_info()))
                    print(stacktrace_str)
                    ingress.parse_fail_info = stacktrace_str
                    ingress.save()

        return success_counter
