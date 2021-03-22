import json
import logging

import pytest
import pytz
from django.conf import settings
from django.test import override_settings
from ingress.models import Message, Collection, FailedMessage

from peoplemeasurement.models import Sensors
from telcameras_v2.ingress_parser import TelcameraParser
from telcameras_v2.models import Observation
from tests.test_telcameras_v2 import TEST_POST

log = logging.getLogger(__name__)
timezone = pytz.timezone("UTC")

AUTHORIZATION_HEADER = {'HTTP_AUTHORIZATION': f"Token {settings.AUTHORIZATION_TOKEN}"}


@pytest.mark.django_db
class TestDataIngressPoster:
    """ Test the second iteration of the api, with the ingress queue """

    @pytest.fixture(autouse=True)
    def setup(self):
        self.collection_name = 'telcameras_v2'
        self.URL = f'/ingress/{self.collection_name}/'

        # Create an endpoint
        self.collection_obj = Collection.objects.create(name=self.collection_name, consumer_enabled=True)

        # Create the sensor in the database
        self.sensor = Sensors.objects.create(objectnummer='GAVM-01-Vondelstraat')

    @pytest.mark.parametrize(
        "store_all_data", [True, False]
    )
    def test_parse_ingress(self, client, store_all_data):
        with override_settings(STORE_ALL_DATA_TELCAMERAS_V2=store_all_data):
            # First add a couple ingress records
            Message.objects.all().delete()
            for i in range(3):
                client.post(self.URL, TEST_POST, **AUTHORIZATION_HEADER, content_type='application/json')
            assert Message.objects.count() == 3

            # Then run the parse_ingress script
            parser = TelcameraParser()
            parser.consume(end_at_empty_queue=True)

            # Test whether the records in the ingress queue are correctly set to parsed
            assert Message.objects.filter(consume_succeeded_at__isnull=False).count() == 3
            assert FailedMessage.objects.count() == 0
            for ingress in Message.objects.all():
                assert ingress.consume_started_at is not None
                assert ingress.consume_succeeded_at is not None

            # Test whether the records were added to the database
            assert Observation.objects.all().count() == 3

    def test_parse_ingress_fail_with_wrong_input(self, client):
        # First add an ingress record which is not correct json
        Message.objects.all().delete()
        client.post(self.URL, "NOT JSON", **AUTHORIZATION_HEADER, content_type='application/json')
        assert Message.objects.count() == 1

        # Then run the parse_ingress script
        parser = TelcameraParser()
        parser.consume(end_at_empty_queue=True)

        # Test whether the record in the ingress queue is moved to the failed queue
        assert Message.objects.count() == 0
        assert FailedMessage.objects.count() == 1
        for failed_ingress in FailedMessage.objects.all():
            assert failed_ingress.consume_started_at is not None
            assert failed_ingress.consume_failed_at is not None
            assert failed_ingress.consume_succeeded_at is None

    @pytest.mark.parametrize(
        "store_all_data,expected_observations", [
            (True, 3),
            (False, 0),
        ]
    )
    def test_data_for_non_existing_sensor(self, client, store_all_data, expected_observations):
        with override_settings(STORE_ALL_DATA_TELCAMERAS_V2=store_all_data):
            # First add a couple ingress records with a non existing sensor code
            Message.objects.all().delete()
            post_data = json.loads(TEST_POST)
            post_data['data'][0]['sensor'] = 'does not exist'
            for i in range(3):
                client.post(self.URL, json.dumps(post_data), **AUTHORIZATION_HEADER, content_type='application/json')

            assert Message.objects.count() == 3

            # Then run the parser
            parser = TelcameraParser()
            parser.consume(end_at_empty_queue=True)

            # Test whether the records in the ingress queue are correctly set to parsed
            assert Message.objects.filter(consume_succeeded_at__isnull=False).count() == 3
            assert FailedMessage.objects.count() == 0
            for ingress in Message.objects.all():
                assert ingress.consume_started_at is not None
                assert ingress.consume_succeeded_at is not None

            # Test whether the records were indeed not added to the database
            assert Observation.objects.all().count() == expected_observations

    @pytest.mark.parametrize("store_all_data,expected_observations", [
        (True, 3),
        (False, 0)
    ])
    def test_data_for_inactive_sensor(self, client, store_all_data, expected_observations):
        with override_settings(STORE_ALL_DATA_TELCAMERAS_V2=store_all_data):
            # First add a couple ingress records with a non existing sensor code
            Message.objects.all().delete()
            for i in range(3):
                client.post(self.URL, TEST_POST, **AUTHORIZATION_HEADER, content_type='application/json')
            assert Message.objects.count() == 3

            # Set the sensor to inactive
            self.sensor.is_active = False
            self.sensor.save()

            # Then run the parser
            parser = TelcameraParser()
            parser.consume(end_at_empty_queue=True)

            # Test whether the records in the ingress queue are correctly set to parsed
            assert Message.objects.filter(consume_succeeded_at__isnull=False).count() == 3
            assert FailedMessage.objects.count() == 0
            for ingress in Message.objects.all():
                assert ingress.consume_started_at is not None
                assert ingress.consume_succeeded_at is not None

            # Test whether the records were indeed not added to the database
            assert Observation.objects.all().count() == expected_observations

            # Set the sensor back to active again
            self.sensor.is_active = True
            self.sensor.save()
