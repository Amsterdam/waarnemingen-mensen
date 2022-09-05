import json
import logging
from datetime import datetime
from decimal import Decimal

import pytest
import pytz
from dateutil import parser as dateparser
from django.conf import settings
from ingress.models import Collection, FailedMessage, Message

from peoplemeasurement.models import Sensors
from telcameras_v2.ingress_parser import TelcameraParser
from telcameras_v2.models import CountAggregate, Observation
from telcameras_v2.serializers import ObservationSerializer

log = logging.getLogger(__name__)
timezone = pytz.timezone("UTC")

AUTHORIZATION_HEADER = {"HTTP_AUTHORIZATION": f"Token {settings.AUTHORIZATION_TOKEN}"}


# In the posts that we receive, all root fields in the objects are the same,
# except for the version, the message (id), the message_type and the aggregate
TEST_POST = """
{
    "data": [
        {
            "sensor": "GAVM-01-Vondelstraat",
            "sensor_type": "3d_camera",
            "sensor_state": "operational",
            "owner": "Gemeente Amsterdam venor",
            "supplier": "Connection Systems",
            "purpose": ["safety", "comfort"],
            "latitude": 52.361081,
            "longitude": 4.873822,
            "interval": 60,
            "timestamp_message": "2020-06-12T15:39:39.701Z",
            "timestamp_start": "2020-06-12T15:05:00.000Z",
            "message": 211,
            "version": "CS_count_0.0.1",
            "message_type": "count",
            "aggregate": [
                {
                    "id": "Line 0",
                    "type": "line",
                    "azimuth": 170,
                    "count_in": 15,
                    "count_out": 12
                },
                {
                    "type": "zone",
                    "id": "Zone 0",
                    "area": 27.5,
                    "geom": "4.3 6.9,9.4 7.3,9.4 1.9,5.2 2.0",
                    "count": 5
                }
            ]
        }, 
        {
            "sensor": "GAVM-01-Vondelstraat",
            "sensor_type": "3d_camera",
            "sensor_state": "operational",
            "owner": "Gemeente Amsterdam venor",
            "supplier": "Connection Systems",
            "purpose": ["safety", "comfort"],
            "latitude": 52.361081,
            "longitude": 4.873822,
            "interval": 60,
            "timestamp_message": "2020-06-12T15:39:39.701Z",
            "timestamp_start": "2020-06-12T15:05:00.000Z",
            "message": 215,
            "version": "CS_person_0.0.1",
            "message_type": "person",
            "aggregate": [
                {
                    "personId": "3560f51a-4978-4b82-ab6f-b421e739230d",
                    "observation_timestamp": "2020-06-12T15:05:00.000Z",
                    "record": 5342,
                    "speed": 1.5,
                    "geom": "0.6 1.9,0.7 1.6,0.8 1.3,1 1,1 0.6,1.1 0.3,1.2 -0.1,1.2 -0.3,1.2 -0.8,1.2 -1.2,1.2 -1.6,1.2 -1.9,1 -2.3,0.9 -2.8,0.7 -3.2,0.6 -3.5 (-1.2 -0.9 -0.7 -0.5 -0.2 0.1 0.3 0.5 0.8 1 1.3 1.5 1.8 2.1 2.3 2.5)",
                    "quality": 80,
                    "distances": [
                        {
                            "personId": "d89d68be-591e-46f4-a546-81a77114e7a9",
                            "observation_timestamp": "2020-06-12T15:05:00.000Z",
                            "distance_to": "3.4 (-1.2)"
                        }, 
                        {
                            "personId": "653042c8-742a-4735-b619-1f8823304f34",
                            "observation_timestamp": "2020-06-12T15:05:00.000Z",
                            "distance_to": "0.5 0.6 0.6 0.7 0.7 0.6 0.7 0.6 0.8 0.8 0.8 0.9 0.8 0.6 (-0.2 0 0.3 0.6 0.8 1 1.3 1.5 1.8 2 2.3 2.6 2.8 3)"
                        }
                    ]
                }, 
                {
                    "personId": "653042c8-742a-4735-b619-1f8823304f34",
                    "observation_timestamp": "2020-06-12T15:05:00.000Z",
                    "record": 5343,
                    "speed": null,
                    "geom": "0.4 1.2,0.4 0.7,0.4 0.2,0.5 0,0.7 -0.5,0.5 -1.1,0.6 -1.3,0.5 -2,0.5 -2.2,0.3 -2.8,0.1 -3.3,0.1 -3.6,0.2 -3.9,0.3 -4.3 (-0.5 -0.2 0.1 0.3 0.5 0.8 1 1.3 1.5 1.8 2.1 2.3 2.5 2.8)",
                    "quality": 80,
                    "distances": [
                        {
                            "personId": "3560f51a-4978-4b82-ab6f-b421e739230d",
                            "observation_timestamp": "2020-06-12T15:05:00.000Z",
                            "distance_to": "0.5 0.6 0.6 0.7 0.7 0.6 0.7 0.6 0.8 0.8 0.8 0.9 0.8 0.6 (-0.7 -0.5 -0.2 0.1 0.3 0.5 0.8 1 1.3 1.5 1.8 2.1 2.3 2.5)"
                        }
                    ]
                }
            ]
        }
    ]
}
"""


TEST_POST_DOUBLE_ZONE = """
{
    "data": [
        {
            "sensor": "GADM-01",
            "sensor_type": "telcamera",
            "message_type": "count",
            "timestamp_message": "2021-02-03T13:57:34.680Z",
            "sensor_state": "operational",
            "version": "CS_count_0.0.1",
            "owner": "Gemeente Amsterdam venor",
            "supplier": "Connection Systems",
            "purpose": [
                "safety",
                "comfort"
            ],
            "latitude": 52.373128,
            "longitude": 4.89305,
            "message": 15749,
            "timestamp_start": "2021-02-03T13:56:00.000Z",
            "interval": 60,
            "aggregate": [
                {
                    "type": "zone",
                    "id": "GADM-01-total",
                    "area": 8100,
                    "geom": "",
                    "count": 2
                },
                {
                    "type": "zone",
                    "id": "GADM-01-zone0",
                    "area": 1563,
                    "geom": "",
                    "count": 1
                }
            ]
        }
    ]
}
"""


@pytest.mark.django_db
class TestDataIngressPoster:
    """Test the second iteration of the api, with the ingress queue"""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.collection_name = "telcameras_v2"
        self.URL = f"/ingress/{self.collection_name}/"

        # Create an endpoint
        self.collection_obj = Collection.objects.create(name=self.collection_name, consumer_enabled=True)

        # Create the sensor in the database
        self.sensor = Sensors.objects.create(objectnummer="GAVM-01-Vondelstraat", gid=1)

    def test_observation_serializer_is_valid(self):
        data = json.loads(TEST_POST)["data"]
        observation = TelcameraParser().data_to_observation(data)
        observation_serializer = ObservationSerializer(data=observation)
        assert observation_serializer.is_valid()

    def test_parse_ingress(self, client):
        # First add a couple ingress records
        Message.objects.all().delete()
        for i in range(3):
            client.post(self.URL, TEST_POST, **AUTHORIZATION_HEADER, content_type="application/json")
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
        client.post(self.URL, "NOT JSON", **AUTHORIZATION_HEADER, content_type="application/json")
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

    def test_data_for_non_existing_sensor(self, client):
        # First add a couple ingress records with a non existing sensor code
        Message.objects.all().delete()
        post_data = json.loads(TEST_POST)
        post_data["data"][0]["sensor"] = "does not exist"
        for i in range(3):
            client.post(self.URL, json.dumps(post_data), **AUTHORIZATION_HEADER, content_type="application/json")

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

    def test_post_new_record_with_double_zone(self, client):
        """Test posting a new message with a double zone in the count message"""
        Message.objects.all().delete()
        post_data = json.loads(TEST_POST_DOUBLE_ZONE)
        client.post(self.URL, json.dumps(post_data), **AUTHORIZATION_HEADER, content_type="application/json")
        assert Message.objects.count() == 1

        # Then run the parser
        parser = TelcameraParser()
        parser.consume(end_at_empty_queue=True)

        # Check the Observation record
        assert Observation.objects.all().count() == 1
        observation = Observation.objects.get()
        fields_to_check = {
            "sensor_name": "sensor",
            "sensor_type": "sensor_type",
            "sensor_state": "sensor_state",
            "owner": "owner",
            "supplier": "supplier",
            "purpose": "purpose",
            "latitude": "latitude",
            "longitude": "longitude",
            "interval": "interval",
            "timestamp_message": "timestamp_message",
            "timestamp_start": "timestamp_start",
        }
        for attr, post_attr in fields_to_check.items():
            if type(getattr(observation, attr)) is Decimal:
                assert float(getattr(observation, attr)) == post_data["data"][0][post_attr]
            elif type(getattr(observation, attr)) is datetime:
                assert getattr(observation, attr) == dateparser.parse(post_data["data"][0][post_attr])
            else:
                assert getattr(observation, attr) == post_data["data"][0][post_attr]

        # Check the CountAggregate records
        assert CountAggregate.objects.all().count() == len(post_data["data"][0]["aggregate"])
        for count_aggr in CountAggregate.objects.all():

            # Get the post data for this CountAggregate (they might not be in the same order)
            posted_count_aggregate = None
            for aggregate in post_data["data"][0]["aggregate"]:
                if aggregate["id"] == count_aggr.external_id:
                    posted_count_aggregate = aggregate

            # Check whether we actually found the correct posted count aggregate
            assert type(posted_count_aggregate) == dict

            aggregates_to_check = {"type": "type", "area_size": "area", "count": "count"}
            for attr, posted_attr in aggregates_to_check.items():
                assert getattr(count_aggr, attr) == posted_count_aggregate[posted_attr]
            assert count_aggr.external_id == posted_count_aggregate["id"]
            assert (
                count_aggr.geom is None
            )  # In both zone counts the geom is an empty string. So we check whether they are None
            assert count_aggr.message == post_data["data"][0]["message"]
            assert count_aggr.version == post_data["data"][0]["version"]

    def test_geom_fields_to_null(self, client):
        post_data = json.loads(TEST_POST)
        post_data["data"][0]["aggregate"][1]["geom"] = None
        post_data["data"][1]["aggregate"][0]["geom"] = None
        post_data["data"][1]["aggregate"][1]["geom"] = None

        Message.objects.all().delete()
        client.post(self.URL, json.dumps(post_data), **AUTHORIZATION_HEADER, content_type="application/json")
        assert Message.objects.count() == 1

        # Then run the parser
        parser = TelcameraParser()
        parser.consume(end_at_empty_queue=True)

        assert Observation.objects.all().count() == 1

    def test_absent_geom_fields(self, client):
        post_data = json.loads(TEST_POST)
        del post_data["data"][0]["aggregate"][1]["geom"]
        del post_data["data"][1]["aggregate"][0]["geom"]
        del post_data["data"][1]["aggregate"][1]["geom"]

        Message.objects.all().delete()
        client.post(self.URL, json.dumps(post_data), **AUTHORIZATION_HEADER, content_type="application/json")
        assert Message.objects.count() == 1

        # Then run the parser
        parser = TelcameraParser()
        parser.consume(end_at_empty_queue=True)

        assert Observation.objects.all().count() == 1

    def test_geom_fields_to_empty_string(self, client):
        post_data = json.loads(TEST_POST)
        post_data["data"][0]["aggregate"][1]["geom"] = ""
        post_data["data"][1]["aggregate"][0]["geom"] = ""
        post_data["data"][1]["aggregate"][1]["geom"] = ""

        Message.objects.all().delete()
        client.post(self.URL, json.dumps(post_data), **AUTHORIZATION_HEADER, content_type="application/json")
        assert Message.objects.count() == 1

        # Then run the parser
        parser = TelcameraParser()
        parser.consume(end_at_empty_queue=True)

        assert Observation.objects.all().count() == 1

    def test_lat_lng_with_many_decimals(self, client):
        post_data = json.loads(TEST_POST)
        post_data["data"][0]["latitude"] = 52.3921439524031
        post_data["data"][0]["longitude"] = 4.885872984800177
        post_data["data"][1]["latitude"] = post_data["data"][0]["latitude"]
        post_data["data"][1]["longitude"] = post_data["data"][0]["longitude"]

        Message.objects.all().delete()
        client.post(self.URL, json.dumps(post_data), **AUTHORIZATION_HEADER, content_type="application/json")
        assert Message.objects.count() == 1

        # Then run the parser
        parser = TelcameraParser()
        parser.consume(end_at_empty_queue=True)

        assert Observation.objects.all().count() == 1

    def test_empty_distances_array(self, client):
        post_data = json.loads(TEST_POST)
        post_data["data"][1]["aggregate"][0]["distances"] = []
        post_data["data"][1]["aggregate"][1]["distances"] = []

        Message.objects.all().delete()
        client.post(self.URL, json.dumps(post_data), **AUTHORIZATION_HEADER, content_type="application/json")
        assert Message.objects.count() == 1

        # Then run the parser
        parser = TelcameraParser()
        parser.consume(end_at_empty_queue=True)

        assert Observation.objects.all().count() == 1

    def test_distances_is_null(self, client):
        post_data = json.loads(TEST_POST)
        post_data["data"][1]["aggregate"][0]["distances"] = None
        post_data["data"][1]["aggregate"][1]["distances"] = None

        Message.objects.all().delete()
        client.post(self.URL, json.dumps(post_data), **AUTHORIZATION_HEADER, content_type="application/json")
        assert Message.objects.count() == 1

        # Then run the parser
        parser = TelcameraParser()
        parser.consume(end_at_empty_queue=True)

        assert Observation.objects.all().count() == 1

    def test_no_distances_key(self, client):
        post_data = json.loads(TEST_POST)
        del post_data["data"][1]["aggregate"][0]["distances"]
        del post_data["data"][1]["aggregate"][1]["distances"]

        Message.objects.all().delete()
        client.post(self.URL, json.dumps(post_data), **AUTHORIZATION_HEADER, content_type="application/json")
        assert Message.objects.count() == 1

        # Then run the parser
        parser = TelcameraParser()
        parser.consume(end_at_empty_queue=True)

        assert Observation.objects.all().count() == 1
