import json
import logging

import pytest
import pytz
from django.conf import settings
from django.test import TestCase
from ingress.models import Collection, FailedMessage, Message
from model_bakery import baker

from peoplemeasurement.models import Sensors
from telcameras_v3.ingress_parser import TelcameraParser
from telcameras_v3.models import GroupAggregate, Observation, Person
from telcameras_v3.serializers import ObservationSerializer
from telcameras_v3.tools import scramble_group_aggregate
from tests.tools_for_testing import call_man_command

log = logging.getLogger(__name__)
timezone = pytz.timezone("UTC")

AUTHORIZATION_HEADER = {"HTTP_AUTHORIZATION": f"Token {settings.AUTHORIZATION_TOKEN}"}

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


TEST_POST_PRORAIL = """
{
    "id": 1,
    "timestamp": "2021-02-16T13:37:00Z",
    "sensor": "CMSA-GAWW-13",
    "sensor_type": "3D sensor",
    "status": "operational",
    "version": "prorail",
    "latitude": 52.394632,
    "longitude": 4.950445,
    "interval": 60,
    "density": null,
    "direction": [
        {
            "azimuth": 120,
            "count": 0,
            "cumulative_distance": null,
            "cumulative_time": null,
            "median_speed": null
        },
        {
            "cumulative_time": null,
            "median_speed": null,
            "count": 0,
            "azimuth": 300,
            "cumulative_distance": null
        }
    ]
}
"""


@pytest.mark.django_db
class TestDataIngressPoster:
    """Test the third iteration of the api with the ingress queue"""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.collection_name = "telcameras_v3"
        self.URL = f"/ingress/{self.collection_name}/"

        # Create an endpoint
        self.collection_obj = Collection.objects.create(
            name=self.collection_name, consumer_enabled=True
        )

        # Create the sensor in the database
        self.sensor = Sensors.objects.create(
            objectnummer=json.loads(TEST_POST)["sensor"], gid=1
        )

    def test_serializer_is_valid(self):
        observation = TelcameraParser().data_to_observation(TEST_POST)
        serializer = ObservationSerializer(data=observation)
        assert serializer.is_valid()

    def test_parse_ingress(self, client):
        # First add a couple ingress records
        Message.objects.all().delete()
        for _ in range(3):
            client.post(
                self.URL,
                TEST_POST,
                **AUTHORIZATION_HEADER,
                content_type="application/json",
            )
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

        # Test whether the records were indeed added to the database
        assert Observation.objects.all().count() == 3
        assert GroupAggregate.objects.all().count() == 9
        assert Person.objects.all().count() == 15

    def test_parse_ingress_prorail(self, client):
        # First add a couple ingress records
        Message.objects.all().delete()
        for _ in range(3):
            client.post(
                self.URL,
                TEST_POST_PRORAIL,
                **AUTHORIZATION_HEADER,
                content_type="application/json",
            )
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

        # Test whether the records were indeed added to the database
        assert Observation.objects.all().count() == 3
        assert GroupAggregate.objects.all().count() == 6
        assert Person.objects.all().count() == 0

    def test_parse_ingress_fail_with_wrong_input(self, client):
        # First add an ingress record which is not correct json
        Message.objects.all().delete()
        client.post(
            self.URL,
            "NOT JSON",
            **AUTHORIZATION_HEADER,
            content_type="application/json",
        )
        assert Message.objects.count() == 1

        # Then run the parse_ingress script
        parser = TelcameraParser()
        parser.consume(end_at_empty_queue=True)

        # Test whether the record in the ingress queue is correctly set to consume_failed_at
        assert Message.objects.count() == 0
        assert FailedMessage.objects.count() == 1
        for failed_ingress in FailedMessage.objects.all():
            assert failed_ingress.consume_started_at is not None
            assert failed_ingress.consume_failed_at is not None
            assert failed_ingress.consume_succeeded_at is None

    def test_data_for_non_existing_sensor(self, client):
        # First add a couple ingress records with a non existing sensor code
        Message.objects.all().delete()
        post_data = json.loads(TEST_POST)
        post_data["sensor"] = "does not exist"
        for _ in range(3):
            client.post(
                self.URL,
                json.dumps(post_data),
                **AUTHORIZATION_HEADER,
                content_type="application/json",
            )
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

        # Test whether the records were added to the database
        assert Observation.objects.all().count() == 3
        assert GroupAggregate.objects.all().count() == 9
        assert Person.objects.all().count() == 15


@pytest.mark.django_db
class TestTools(TestCase):
    def test_scramble_count_vanilla(self):
        count_agg = baker.make(GroupAggregate)
        count_agg.count = 1
        count_agg.count_scrambled = None

        count_agg = scramble_group_aggregate(count_agg)

        assert count_agg.count_scrambled in (0, 1, 2)

    def test_scramble_count_with_counts_none(self):
        count_agg = baker.make(GroupAggregate)
        count_agg.count = None
        count_agg.count_scrambled = None

        count_agg = scramble_group_aggregate(count_agg)

        assert count_agg.count_scrambled is None

    def test_scramble_count_doesnt_overwrite(self):
        count_agg = baker.make(GroupAggregate)
        count_agg.count = 1
        count_agg.count_scrambled = 1

        count_agg = scramble_group_aggregate(count_agg)

        assert count_agg.count_scrambled == 1

    def test_scramble_count_with_counts_zero(self):
        count_agg = baker.make(GroupAggregate)
        count_agg.count = 0
        count_agg.count_scrambled = None

        count_agg = scramble_group_aggregate(count_agg)
        self.assertIn(count_agg.count_scrambled, (0, 1))

    def test_scramble_v3_counts_command(self):
        from random import randint

        from django.db.models import F, Q
        from model_bakery.recipe import Recipe

        group_aggregate_recipe = Recipe(
            GroupAggregate,
            count=randint(0, 1000),
            count_scrambled=None,
        )
        group_aggregate_recipe.make(_quantity=100)

        for ga in GroupAggregate.objects.all():
            self.assertIsNotNone(ga.count)
            self.assertIsNone(ga.count_scrambled)

        # Do the scrambling
        call_man_command("scramble_v3_counts")

        differ_count = 0
        for ga in GroupAggregate.objects.all():
            self.assertIsNotNone(ga.count_scrambled)
            self.assertIn(ga.count_scrambled, (ga.count - 1, ga.count, ga.count + 1))
            if ga.count_scrambled != ga.count:
                differ_count += 1

        # check all records have their scrambled counts set
        assert not GroupAggregate.objects.filter(count_scrambled=None).exists()

        # check that all scrambled counts are within valid range
        assert not GroupAggregate.objects.filter(
            Q(count_scrambled__gt=F("count") + 1)
            | Q(count_scrambled__lt=F("count") - 1)
        )

        # Make sure that a significant amount of counts_scrambled were actually changed from the original
        self.assertGreater(differ_count, 50)
