import json
import logging

import pytz
from django.conf import settings
from django.test import override_settings
from rest_framework.test import APITestCase

from ingress.models import Endpoint, IngressQueue
from peoplemeasurement.models import Sensors
from telcameras_v2.ingress_parser import TelcameraParser
from telcameras_v2.models import Observation
from tests.test_telcameras_v2 import TEST_POST

log = logging.getLogger(__name__)
timezone = pytz.timezone("UTC")

AUTHORIZATION_HEADER = {'HTTP_AUTHORIZATION': f"Token {settings.AUTHORIZATION_TOKEN}"}


class DataIngressPosterTest(APITestCase):
    """ Test the second iteration of the api, with the ingress queue """

    def setUp(self):
        self.endpoint_url_key = 'telcameras_v2'
        self.URL = '/ingress/' + self.endpoint_url_key

        # Create an endpoint
        self.endpoint_obj = Endpoint.objects.create(url_key=self.endpoint_url_key, parser_enabled=True)

        # Create the sensor in the database
        self.sensor = Sensors.objects.create(objectnummer='GAVM-01-Vondelstraat')

    @override_settings(STORE_ALL_DATA_TELCAMERAS_V2=True)  # It is by default true, but to make it explicit I also override it here
    def test_data_for_non_existing_sensor_is_added_to_the_db_if_STORE_ALL_DATA_is_true(self):
        # First add a couple ingress records with a non existing sensor code
        IngressQueue.objects.all().delete()
        post_data = json.loads(TEST_POST)
        post_data['data'][0]['sensor'] = 'does not exist'
        for i in range(3):
            self.client.post(self.URL, json.dumps(post_data), **AUTHORIZATION_HEADER, content_type='application/json')
        self.assertEqual(IngressQueue.objects.count(), 3)

        # Then run the parser
        parser = TelcameraParser()
        parser.parse_continuously(end_at_empty_queue=True)

        # Test whether the records in the ingress queue are correctly set to parsed
        self.assertEqual(IngressQueue.objects.filter(parse_succeeded__isnull=False).count(), 3)
        for ingress in IngressQueue.objects.all():
            self.assertIsNotNone(ingress.parse_started)
            self.assertIsNotNone(ingress.parse_succeeded)
            self.assertIsNone(ingress.parse_failed)

        # Test whether the records were added to the database
        self.assertEqual(Observation.objects.all().count(), 3)

    @override_settings(STORE_ALL_DATA_TELCAMERAS_V2=True)
    def test_data_for_inactive_sensor_is_added_to_the_db_if_STORE_ALL_DATA_is_true(self):
        # First add a couple ingress records with a non existing sensor code
        IngressQueue.objects.all().delete()
        for i in range(3):
            self.client.post(self.URL, TEST_POST, **AUTHORIZATION_HEADER, content_type='application/json')
        self.assertEqual(IngressQueue.objects.count(), 3)

        # Set the sensor to inactive
        self.sensor.is_active = False
        self.sensor.save()

        # Then run the parser
        parser = TelcameraParser()
        parser.parse_continuously(end_at_empty_queue=True)

        # Test whether the records in the ingress queue are correctly set to parsed
        self.assertEqual(IngressQueue.objects.filter(parse_succeeded__isnull=False).count(), 3)
        for ingress in IngressQueue.objects.all():
            self.assertIsNotNone(ingress.parse_started)
            self.assertIsNotNone(ingress.parse_succeeded)
            self.assertIsNone(ingress.parse_failed)

        # Test whether the records were added to the database
        self.assertEqual(Observation.objects.all().count(), 3)

        # Set the sensor back to active again
        self.sensor.is_active = True
        self.sensor.save()

    @override_settings(STORE_ALL_DATA_TELCAMERAS_V2=False)
    def test_data_for_non_existing_sensor_is_not_added_to_the_db(self):
        # First add a couple ingress records with a non existing sensor code
        IngressQueue.objects.all().delete()
        post_data = json.loads(TEST_POST)
        post_data['data'][0]['sensor'] = 'does not exist'
        for i in range(3):
            self.client.post(self.URL, json.dumps(post_data), **AUTHORIZATION_HEADER, content_type='application/json')
        self.assertEqual(IngressQueue.objects.count(), 3)

        # Then run the parser
        parser = TelcameraParser()
        parser.parse_continuously(end_at_empty_queue=True)

        # Test whether the records in the ingress queue are correctly set to parsed
        self.assertEqual(IngressQueue.objects.filter(parse_succeeded__isnull=False).count(), 3)
        for ingress in IngressQueue.objects.all():
            self.assertIsNotNone(ingress.parse_started)
            self.assertIsNotNone(ingress.parse_succeeded)
            self.assertIsNone(ingress.parse_failed)

        # Test whether the records were indeed not added to the database
        self.assertEqual(Observation.objects.all().count(), 0)

    @override_settings(STORE_ALL_DATA_TELCAMERAS_V2=False)
    def test_data_for_inactive_sensor_is_not_added_to_the_db(self):
        # First add a couple ingress records with a non existing sensor code
        IngressQueue.objects.all().delete()
        for i in range(3):
            self.client.post(self.URL, TEST_POST, **AUTHORIZATION_HEADER, content_type='application/json')
        self.assertEqual(IngressQueue.objects.count(), 3)

        # Set the sensor to inactive
        self.sensor.is_active = False
        self.sensor.save()

        # Then run the parser
        parser = TelcameraParser()
        parser.parse_continuously(end_at_empty_queue=True)

        # Test whether the records in the ingress queue are correctly set to parsed
        self.assertEqual(IngressQueue.objects.filter(parse_succeeded__isnull=False).count(), 3)
        for ingress in IngressQueue.objects.all():
            self.assertIsNotNone(ingress.parse_started)
            self.assertIsNotNone(ingress.parse_succeeded)
            self.assertIsNone(ingress.parse_failed)

        # Test whether the records were indeed not added to the database
        self.assertEqual(Observation.objects.all().count(), 0)

        # Set the sensor back to active again
        self.sensor.is_active = True
        self.sensor.save()

    def test_parse_ingress(self):
        # First add a couple ingress records
        IngressQueue.objects.all().delete()
        for i in range(3):
            self.client.post(self.URL, TEST_POST, **AUTHORIZATION_HEADER, content_type='application/json')
        self.assertEqual(IngressQueue.objects.count(), 3)

        # Then run the parse_ingress script
        parser = TelcameraParser()
        parser.parse_continuously(end_at_empty_queue=True)

        # Test whether the records in the ingress queue are correctly set to parsed
        self.assertEqual(IngressQueue.objects.filter(parse_succeeded__isnull=False).count(), 3)
        for ingress in IngressQueue.objects.all():
            self.assertIsNotNone(ingress.parse_started)
            self.assertIsNotNone(ingress.parse_succeeded)
            self.assertIsNone(ingress.parse_failed)

        # Test whether the records were added to the database
        self.assertEqual(Observation.objects.all().count(), 3)

    def test_parse_ingress_fail(self):
        # First add an ingress record which is not correct json
        IngressQueue.objects.all().delete()
        self.client.post(self.URL, "NOT JSON", **AUTHORIZATION_HEADER, content_type='application/json')
        self.assertEqual(IngressQueue.objects.count(), 1)

        # Then run the parse_ingress script
        parser = TelcameraParser()
        parser.parse_continuously(end_at_empty_queue=True)

        # Test whether the record in the ingress queue is correctly set to parse_failed
        for ingress in IngressQueue.objects.all():
            self.assertIsNotNone(ingress.parse_started)
            self.assertIsNone(ingress.parse_succeeded)
            self.assertIsNotNone(ingress.parse_failed)
