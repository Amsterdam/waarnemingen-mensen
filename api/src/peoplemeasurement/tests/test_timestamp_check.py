from django.utils import timezone
from rest_framework.test import APITestCase
from django.core.management import call_command

from .factories import PeopleMeasurementFactory


class TestTimestampCheck(APITestCase):

    def test_database_is_empty(self):
        with self.assertRaises(Exception):
            call_command('peoplemeasurement_timestamp_check')

    def test_timestamp_old(self):
        PeopleMeasurementFactory(timestamp=timezone.now() - timezone.timedelta(hours=2))

        with self.assertRaises(AssertionError):
            call_command('peoplemeasurement_timestamp_check')

    def test_timestamp_ok(self):
        PeopleMeasurementFactory()
        call_command('peoplemeasurement_timestamp_check')

