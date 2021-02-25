import json
import logging

import pytest
import pytz
from django.conf import settings
from django.test import override_settings
from ingress.models import Collection, Message, FailedMessage

from centralerekenapplicatie_v1.ingress_parser import MetricParser
from centralerekenapplicatie_v1.models import (AreaMetric, LineMetric,
                                               LineMetricCount)
from peoplemeasurement.models import Sensors

log = logging.getLogger(__name__)
timezone = pytz.timezone("UTC")

AUTHORIZATION_HEADER = {'HTTP_AUTHORIZATION': f"Token {settings.AUTHORIZATION_TOKEN}"}


TEST_POST_AREA = """
{
    "id": "Zone 0",
    "area": 74.6,
    "count": 1,
    "density": 0.013405,
    "totalDistance": 4.2758,
    "totalTime": 3.1,
    "speed": 1.3793,
    "source": {
        "sensor": "GABM-03",
        "timestamp": "2021-01-18T00:28:00Z",
        "originalId": 147751,
        "adminId": 1
    },
    "type": "areaMetrics"
}
"""

TEST_POST_LINE = """
{
    "id": "Line 0",
    "counts": [{
        "azimuth": 50.0,
        "count": 1
    }, {
        "azimuth": 230.0,
        "count": 0
    }],
    "source": {
        "sensor": "GACM-04",
        "timestamp": "2021-01-18T00:28:00Z",
        "originalId": 164240,
        "adminId": 1
    },
    "type": "lineMetrics"
}
"""


@pytest.mark.django_db
class TestDataIngressPoster:

    @pytest.fixture(autouse=True)
    def setup(self):
        self.collection_name = 'centralerekenapplicatie'
        self.URL = '/ingress/' + self.collection_name

        # Create a collection
        self.collection_obj = Collection.objects.create(name=self.collection_name, consumer_enabled=True)

        # Create the sensors in the database
        self.sensor_area = Sensors.objects.create(objectnummer=json.loads(TEST_POST_AREA)['source']['sensor'])
        self.sensor_line = Sensors.objects.create(objectnummer=json.loads(TEST_POST_LINE)['source']['sensor'])

    def test_parse_ingress(self, client):
        # First add a couple ingress records
        Message.objects.all().delete()
        for _ in range(3):
            client.post(self.URL, TEST_POST_AREA, **AUTHORIZATION_HEADER, content_type='application/json')
            client.post(self.URL, TEST_POST_LINE, **AUTHORIZATION_HEADER, content_type='application/json')
        assert Message.objects.count() == 6

        # Then run the parse_ingress script
        parser = MetricParser()
        parser.consume(end_at_empty_queue=True)

        # Test whether the records in the ingress queue are correctly set to parsed
        assert Message.objects.filter(consume_succeeded_at__isnull=False).count() == 6
        assert FailedMessage.objects.count() == 0
        for ingress in Message.objects.all():
            assert ingress.consume_started_at is not None
            assert ingress.consume_succeeded_at is not None

        # Test whether the records were added to the database
        assert AreaMetric.objects.all().count() == 3
        assert LineMetric.objects.all().count() == 3
        assert LineMetricCount.objects.all().count() == 6

    def test_parse_ingress_fail_with_wrong_input(self, client):
        # First add an ingress record which is not correct json
        Message.objects.all().delete()
        client.post(self.URL, "NOT JSON", **AUTHORIZATION_HEADER, content_type='application/json')
        assert Message.objects.count() == 1

        # Then run the parse_ingress script
        parser = MetricParser()
        parser.consume(end_at_empty_queue=True)

        # Test whether the record in the ingress queue is correctly set to consume_failed_at
        assert Message.objects.count() == 0
        assert FailedMessage.objects.count() == 1
        for failed_ingress in Message.objects.all():
            assert failed_ingress.consume_started_at is not None
            assert failed_ingress.consume_failed_at is not None
            assert failed_ingress.consume_succeeded_at is None

    @override_settings(STORE_ALL_DATA_CRA=True)  # It is by default true, but to make it explicit I also override it here
    def test_data_for_non_existing_sensor_is_added_to_the_db_if_STORE_ALL_DATA_is_true(self, client):
        # First add a couple ingress records with a non existing sensor code
        Message.objects.all().delete()
        post_data = json.loads(TEST_POST_LINE)
        post_data['source']['sensor'] = 'does not exist'
        for _ in range(3):
            client.post(self.URL, json.dumps(post_data), **AUTHORIZATION_HEADER, content_type='application/json')
        assert Message.objects.count() == 3

        # Then run the parser
        parser = MetricParser()
        parser.consume(end_at_empty_queue=True)

        # Test whether the records in the ingress queue are correctly set to parsed
        assert Message.objects.filter(consume_succeeded_at__isnull=False).count() == 3
        assert FailedMessage.objects.count() == 0
        for ingress in Message.objects.all():
            assert ingress.consume_started_at is not None
            assert ingress.consume_succeeded_at is not None

        # Test whether the records were added to the database
        assert LineMetric.objects.all().count() == 3
        assert LineMetricCount.objects.all().count() == 6

    @override_settings(STORE_ALL_DATA_CRA=True)
    def test_data_for_inactive_sensor_is_added_to_the_db_if_STORE_ALL_DATA_is_true(self, client):
        # First add a couple ingress records with a non existing sensor code
        Message.objects.all().delete()
        for _ in range(3):
            client.post(self.URL, TEST_POST_AREA, **AUTHORIZATION_HEADER, content_type='application/json')
            client.post(self.URL, TEST_POST_LINE, **AUTHORIZATION_HEADER, content_type='application/json')
        assert Message.objects.count() == 6

        # Set the sensor to inactive
        self.sensor_area.is_active = False
        self.sensor_area.save()
        self.sensor_line.is_active = False
        self.sensor_line.save()

        # Then run the parser
        parser = MetricParser()
        parser.consume(end_at_empty_queue=True)

        # Test whether the records in the ingress queue are correctly set to parsed
        assert Message.objects.filter(consume_succeeded_at__isnull=False).count() == 6
        assert FailedMessage.objects.count() == 0
        for ingress in Message.objects.all():
            assert ingress.consume_started_at is not None
            assert ingress.consume_succeeded_at is not None

        # Test whether the records were added to the database
        assert AreaMetric.objects.all().count() == 3
        assert LineMetric.objects.all().count() == 3
        assert LineMetricCount.objects.all().count() == 6

        # Set the sensor back to active again
        self.sensor_area.is_active = True
        self.sensor_area.save()
        self.sensor_line.is_active = True
        self.sensor_line.save()

    @override_settings(STORE_ALL_DATA_CRA=False)
    def test_data_for_existing_sensor_is_added_to_the_db(self, client):
        # First add a couple ingress records
        Message.objects.all().delete()
        for _ in range(3):
            client.post(self.URL, TEST_POST_AREA, **AUTHORIZATION_HEADER, content_type='application/json')
            client.post(self.URL, TEST_POST_LINE, **AUTHORIZATION_HEADER, content_type='application/json')
        assert Message.objects.count() == 6

        # Then run the parser
        parser = MetricParser()
        parser.consume(end_at_empty_queue=True)

        # Test whether the records in the ingress queue are correctly set to parsed
        assert Message.objects.filter(consume_succeeded_at__isnull=False).count() == 6
        assert FailedMessage.objects.count() == 0
        for ingress in Message.objects.all():
            assert ingress.consume_started_at is not None
            assert ingress.consume_succeeded_at is not None

        # Test whether the records were indeed added to the database
        assert AreaMetric.objects.all().count() == 3
        assert LineMetric.objects.all().count() == 3
        assert LineMetricCount.objects.all().count() == 6

    @override_settings(STORE_ALL_DATA_CRA=False)
    def test_data_for_non_existing_sensor_is_not_added_to_the_db(self, client):
        # First add a couple ingress records with a non existing sensor code
        Message.objects.all().delete()
        post_data_area = json.loads(TEST_POST_AREA)
        post_data_line = json.loads(TEST_POST_LINE)
        post_data_area['source']['sensor'] = 'does not exist'
        post_data_line['source']['sensor'] = 'does not exist'
        for _ in range(3):
            client.post(self.URL, json.dumps(post_data_area), **AUTHORIZATION_HEADER, content_type='application/json')
            client.post(self.URL, json.dumps(post_data_line), **AUTHORIZATION_HEADER, content_type='application/json')
        assert Message.objects.count() == 6

        # Then run the parser
        parser = MetricParser()
        parser.consume(end_at_empty_queue=True)

        # Test whether the records in the ingress queue are correctly set to parsed
        assert Message.objects.filter(consume_succeeded_at__isnull=False).count() == 6
        assert FailedMessage.objects.count() == 0
        for ingress in Message.objects.all():
            assert ingress.consume_started_at is not None
            assert ingress.consume_succeeded_at is not None

        # Test whether the records were indeed not added to the database
        assert AreaMetric.objects.all().count() == 0
        assert LineMetric.objects.all().count() == 0
        assert LineMetricCount.objects.all().count() == 0

    @override_settings(STORE_ALL_DATA_CRA=False)
    def test_data_for_inactive_sensor_is_not_added_to_the_db(self, client):
        # First add a couple ingress records with a non existing sensor code
        Message.objects.all().delete()
        for _ in range(3):
            client.post(self.URL, TEST_POST_AREA, **AUTHORIZATION_HEADER, content_type='application/json')
            client.post(self.URL, TEST_POST_LINE, **AUTHORIZATION_HEADER, content_type='application/json')
        assert Message.objects.count() == 6

        # Set the sensor to inactive
        self.sensor_area.is_active = False
        self.sensor_area.save()
        self.sensor_line.is_active = False
        self.sensor_line.save()

        # Then run the parser
        parser = MetricParser()
        parser.consume(end_at_empty_queue=True)

        # Test whether the records in the ingress queue are correctly set to parsed
        assert Message.objects.filter(consume_succeeded_at__isnull=False).count() == 6
        assert FailedMessage.objects.count() == 0
        for ingress in Message.objects.all():
            assert ingress.consume_started_at is not None
            assert ingress.consume_succeeded_at is not None

        # Test whether the records were indeed not added to the database
        assert AreaMetric.objects.all().count() == 0
        assert LineMetric.objects.all().count() == 0
        assert LineMetricCount.objects.all().count() == 0

        # Set the sensor back to active again
        self.sensor_area.is_active = True
        self.sensor_area.save()
        self.sensor_line.is_active = True
        self.sensor_line.save()
