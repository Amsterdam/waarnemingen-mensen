from django.utils import timezone
from rest_framework.test import APITestCase
from django.core.management import call_command

from .factories import PassageFactory


class TestTimestampCheck(APITestCase):

    def test_database_is_empty(self):
        with self.assertRaises(Exception):
            call_command('passage_timestamp_check')

    def test_timestamp_old(self):
        instance = PassageFactory()
        instance.created_at = timezone.now() - timezone.timedelta(hours=2)
        instance.save()

        with self.assertRaises(AssertionError):
            call_command('passage_timestamp_check')

    def test_timestamp_ok(self):
        PassageFactory()
        call_command('passage_timestamp_check')
