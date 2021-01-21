import json
import logging

import pytz
from django.conf import settings
from django.test import TestCase, override_settings
from model_bakery import baker
from rest_framework.test import APITestCase

from ingress.models import Endpoint, IngressQueue
from peoplemeasurement.models import Sensors
from telcameras_v3.ingress_parser import TelcameraParser
from telcameras_v3.models import GroupAggregate, Observation, Person
from telcameras_v3.tools import scramble_group_aggregate

log = logging.getLogger(__name__)
timezone = pytz.timezone("UTC")

AUTHORIZATION_HEADER = {'HTTP_AUTHORIZATION': f"Token {settings.AUTHORIZATION_TOKEN}"}

TEST_POST = """
{
    "id": 26790757,
    "timestamp": "2020-12-08T16:37:00Z",
    "sensor": "ABC-123",
    "sensor_type": "counting_camera",
    "status": "operational",
    "version": "1.2.3.4",
    "latitude": 52.123456,
    "longitude": 4.123456,
    "interval": 60,
    "density": 0,
    "direction": [
        {
            "azimuth": 300,
            "count": 0,
            "cumulative_distance": 0.0,
            "cumulative_time": 0.0,
            "median_speed": 0.0
        },
        {
            "azimuth": 226,
            "count": 1,
            "cumulative_distance": 13.5149,
            "cumulative_time": 10.359,
            "median_speed": 1.67051,
            "signals": [
                {
                    "record": "7f62e5d2-ea48-402f-bcf5-ce76ef46223a",
                    "distance": 13.5149,
                    "time": 10.359,
                    "speed": 1.67051,
                    "observation_timestamp": "2020-12-08T16:37:11.175Z",
                    "type": "pedestrian"
                }
            ]
        },
        {
            "azimuth": 38,
            "count": 4,
            "cumulative_distance": 57.5561,
            "cumulative_time": 47.117,
            "median_speed": 1.77449,
            "signals": [
                {
                    "record": "a2bb02cf-54c0-407d-a759-e9b2ecf6ff3b",
                    "distance": 14.8144,
                    "time": 12.4,
                    "speed": 1.61492,
                    "observation_timestamp": "2020-12-08T16:37:01.935Z",
                    "type": "pedestrian"
                },
                {
                    "record": "a6da6ca8-4d3b-4380-b020-026413774f4c",
                    "distance": 14.4826,
                    "time": 12.479,
                    "speed": 2.05768,
                    "observation_timestamp": "2020-12-08T16:37:02.615Z",
                    "type": "pedestrian"
                },
                {
                    "record": "cf5a1301-d0bd-46e8-94ea-ecc928bbb45d",
                    "distance": 13.8678,
                    "time": 12.479,
                    "speed": 1.54313,
                    "observation_timestamp": "2020-12-08T16:37:17.735Z",
                    "type": "pedestrian"
                },
                {
                    "record": "0a18e31d-93d8-4d8c-acb5-943973188ebe",
                    "distance": 14.3914,
                    "time": 9.759,
                    "speed": 1.93407,
                    "observation_timestamp": "2020-12-08T16:37:59.295Z",
                    "type": "cyclist"
                }
            ]
        }
    ]
}
"""


class DataIngressPosterTest(APITestCase):
    """ Test the third iteration of the api with the ingress queue"""

    def setUp(self):
        self.endpoint_url_key = 'telcameras_v3'
        self.URL = '/ingress/' + self.endpoint_url_key

        # Create an endpoint
        self.endpoint_obj = Endpoint.objects.create(url_key=self.endpoint_url_key, parser_enabled=True)

        # Create the sensor in the database
        self.sensor = Sensors.objects.create(objectnummer=json.loads(TEST_POST)['sensor'])

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
        self.assertEqual(IngressQueue.objects.all().count(), 3)
        for ingress in IngressQueue.objects.all():
            self.assertIsNotNone(ingress.parse_started)
            self.assertIsNotNone(ingress.parse_succeeded)
            self.assertIsNone(ingress.parse_failed)

        # Test whether the records were added to the database
        self.assertEqual(Observation.objects.all().count(), 3)
        self.assertEqual(GroupAggregate.objects.all().count(), 9)
        self.assertEqual(Person.objects.all().count(), 15)

    def test_parse_ingress_fail_with_wrong_input(self):
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

    @override_settings(STORE_ALL_DATA=True)  # It is by default true, but to make it explicit I also override it here
    def test_data_for_non_existing_sensor_is_added_to_the_db_if_STORE_ALL_DATA_is_true(self):
        # First add a couple ingress records with a non existing sensor code
        IngressQueue.objects.all().delete()
        post_data = json.loads(TEST_POST)
        post_data['sensor'] = 'does not exist'
        for _ in range(3):
            self.client.post(self.URL, json.dumps(post_data), **AUTHORIZATION_HEADER, content_type='application/json')
        self.assertEqual(IngressQueue.objects.count(), 3)

        # Then run the parser
        parser = TelcameraParser()
        parser.parse_continuously(end_at_empty_queue=True)

        # Test whether the records in the ingress queue are correctly set to parsed
        self.assertEqual(IngressQueue.objects.all().count(), 3)
        for ingress in IngressQueue.objects.all():
            self.assertIsNotNone(ingress.parse_started)
            self.assertIsNotNone(ingress.parse_succeeded)
            self.assertIsNone(ingress.parse_failed)

        # Test whether the records were added to the database
        self.assertEqual(Observation.objects.all().count(), 3)
        self.assertEqual(GroupAggregate.objects.all().count(), 9)
        self.assertEqual(Person.objects.all().count(), 15)

    @override_settings(STORE_ALL_DATA=True)
    def test_data_for_inactive_sensor_is_added_to_the_db_if_STORE_ALL_DATA_is_true(self):
        # First add a couple ingress records with a non existing sensor code
        IngressQueue.objects.all().delete()
        for _ in range(3):
            self.client.post(self.URL, TEST_POST, **AUTHORIZATION_HEADER, content_type='application/json')
        self.assertEqual(IngressQueue.objects.count(), 3)

        # Set the sensor to inactive
        self.sensor.is_active = False
        self.sensor.save()

        # Then run the parser
        parser = TelcameraParser()
        parser.parse_continuously(end_at_empty_queue=True)

        # Test whether the records in the ingress queue are correctly set to parsed
        self.assertEqual(IngressQueue.objects.all().count(), 3)
        for ingress in IngressQueue.objects.all():
            self.assertIsNotNone(ingress.parse_started)
            self.assertIsNotNone(ingress.parse_succeeded)
            self.assertIsNone(ingress.parse_failed)

        # Test whether the records were added to the database
        self.assertEqual(Observation.objects.all().count(), 3)
        self.assertEqual(GroupAggregate.objects.all().count(), 9)
        self.assertEqual(Person.objects.all().count(), 15)

        # Set the sensor back to active again
        self.sensor.is_active = False
        self.sensor.save()

    @override_settings(STORE_ALL_DATA=False)
    def test_data_for_existing_sensor_is_added_to_the_db(self):
        # First add a couple ingress records with a non existing sensor code
        IngressQueue.objects.all().delete()
        post_data = json.loads(TEST_POST)
        for _ in range(3):
            self.client.post(self.URL, json.dumps(post_data), **AUTHORIZATION_HEADER, content_type='application/json')
        self.assertEqual(IngressQueue.objects.count(), 3)

        # Then run the parser
        parser = TelcameraParser()
        parser.parse_continuously(end_at_empty_queue=True)

        # Test whether the records in the ingress queue are correctly set to parsed
        self.assertEqual(IngressQueue.objects.all().count(), 3)
        for ingress in IngressQueue.objects.all():
            self.assertIsNotNone(ingress.parse_started)
            self.assertIsNotNone(ingress.parse_succeeded)
            self.assertIsNone(ingress.parse_failed)

        # Test whether the records were indeed not added to the database
        self.assertEqual(Observation.objects.all().count(), 3)
        self.assertEqual(GroupAggregate.objects.all().count(), 9)
        self.assertEqual(Person.objects.all().count(), 15)

    @override_settings(STORE_ALL_DATA=False)
    def test_data_for_non_existing_sensor_is_not_added_to_the_db(self):
        # First add a couple ingress records with a non existing sensor code
        IngressQueue.objects.all().delete()
        post_data = json.loads(TEST_POST)
        post_data['sensor'] = 'does not exist'
        for _ in range(3):
            self.client.post(self.URL, json.dumps(post_data), **AUTHORIZATION_HEADER, content_type='application/json')
        self.assertEqual(IngressQueue.objects.count(), 3)

        # Then run the parser
        parser = TelcameraParser()
        parser.parse_continuously(end_at_empty_queue=True)

        # Test whether the records in the ingress queue are correctly set to parsed
        self.assertEqual(IngressQueue.objects.all().count(), 3)
        for ingress in IngressQueue.objects.all():
            self.assertIsNotNone(ingress.parse_started)
            self.assertIsNotNone(ingress.parse_succeeded)
            self.assertIsNone(ingress.parse_failed)

        # Test whether the records were indeed not added to the database
        self.assertEqual(Observation.objects.all().count(), 0)
        self.assertEqual(GroupAggregate.objects.all().count(), 0)
        self.assertEqual(Person.objects.all().count(), 0)

    @override_settings(STORE_ALL_DATA=False)
    def test_data_for_inactive_sensor_is_not_added_to_the_db(self):
        # First add a couple ingress records with a non existing sensor code
        IngressQueue.objects.all().delete()
        for _ in range(3):
            self.client.post(self.URL, TEST_POST, **AUTHORIZATION_HEADER, content_type='application/json')
        self.assertEqual(IngressQueue.objects.count(), 3)

        # Set the sensor to inactive
        self.sensor.is_active = False
        self.sensor.save()

        # Then run the parser
        parser = TelcameraParser()
        parser.parse_continuously(end_at_empty_queue=True)

        # Test whether the records in the ingress queue are correctly set to parsed
        self.assertEqual(IngressQueue.objects.all().count(), 3)
        for ingress in IngressQueue.objects.all():
            self.assertIsNotNone(ingress.parse_started)
            self.assertIsNotNone(ingress.parse_succeeded)
            self.assertIsNone(ingress.parse_failed)

        # Test whether the records were indeed not added to the database
        self.assertEqual(Observation.objects.all().count(), 0)
        self.assertEqual(GroupAggregate.objects.all().count(), 0)
        self.assertEqual(Person.objects.all().count(), 0)

        # Set the sensor back to active again
        self.sensor.is_active = False
        self.sensor.save()


class ToolsTest(TestCase):
    def test_scramble_count_vanilla(self):
        count_agg = baker.make(GroupAggregate)
        count_agg.count = 1
        count_agg.count_scrambled = None

        count_agg = scramble_group_aggregate(count_agg)

        self.assertIn(count_agg.count_scrambled, (0, 1, 2))

    def test_scramble_count_with_counts_none(self):
        count_agg = baker.make(GroupAggregate)
        count_agg.count = None
        count_agg.count_scrambled = None

        count_agg = scramble_group_aggregate(count_agg)

        self.assertIsNone(count_agg.count_scrambled)

    def test_scramble_count_doesnt_overwrite(self):
        count_agg = baker.make(GroupAggregate)
        count_agg.count = 1
        count_agg.count_scrambled = 1

        count_agg = scramble_group_aggregate(count_agg)

        self.assertEquals(count_agg.count_scrambled, 1)

    def test_scramble_count_with_counts_zero(self):
        count_agg = baker.make(GroupAggregate)
        count_agg.count = 0
        count_agg.count_scrambled = None

        count_agg = scramble_group_aggregate(count_agg)

        self.assertIn(count_agg.count_scrambled, (0, 1))
