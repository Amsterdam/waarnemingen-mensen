import logging

from django.utils import timezone

log = logging.getLogger(__name__)

def verify_timestamp(timestamp):
    latest_is_recent = timezone.now() - timestamp < timezone.timedelta(hours=1)
    assert latest_is_recent, 'Last record was more than an hour ago. Please check the status of the providor'
    log.info('Timestamp OK')
