import logging

import pytz
from rest_framework.test import APITestCase

from ingress.models import IngressQueue, Endpoint
from tests.test_telcameras_v2 import TEST_POST
from tests.tools_for_testing import call_man_command

log = logging.getLogger(__name__)
timezone = pytz.timezone("UTC")


class DataIngressPosterTest(APITestCase):
    """ Test the second iteration of the api, with the ingress queue """

    def setUp(self):
        self.endpoint_url_key = 'telcameras_v2'
        self.URL = '/ingress/' + self.endpoint_url_key

        # Create an endpoint
        self.endpoint_obj = Endpoint.objects.create(url_key = self.endpoint_url_key)


    def test_parse_ingress(self):
        # First add a couple ingress records
        IngressQueue.objects.all().delete()
        for i in range(3):
            self.client.post(self.URL, TEST_POST, format='json')
        self.assertEqual(IngressQueue.objects.count(), 3)

        # Then run the parse_ingress script
        out = call_man_command('parse_ingress')
        self.assertEqual(out.strip(), '3')

        # Test whether the records in the ingress queue are correctly set to parsed
        for ingress in IngressQueue.objects.all():
            self.assertIsNotNone(ingress.parse_started)
            self.assertIsNotNone(ingress.parse_succeeded)
            self.assertIsNone(ingress.parse_failed)

    def test_parse_ingress_fail(self):
        # First add an ingress record which is not correct json
        IngressQueue.objects.all().delete()
        self.client.post(self.URL, "NOT JSON", format='json')
        self.assertEqual(IngressQueue.objects.count(), 1)

        # Then run the parse_ingress script
        out = call_man_command('parse_ingress')
        self.assertEqual(out.strip(), '0')

        # Test whether the record in the ingress queue is correctly set to parse_failed
        for ingress in IngressQueue.objects.all():
            self.assertIsNotNone(ingress.parse_started)
            self.assertIsNone(ingress.parse_succeeded)
            self.assertIsNotNone(ingress.parse_failed)
