import json
import logging
from datetime import date, datetime, timedelta

import pytest
import pytz
from ingress.models import Collection, Message

from continuousaggregate.models import Cmsa15Min
from peoplemeasurement.models import Sensors
from telcameras_v2.ingress_parser import TelcameraParser
from telcameras_v2.models import Observation
from tests.test_telcameras_v2_ingress import AUTHORIZATION_HEADER, TEST_POST
from tests.tools_for_testing import call_man_command

log = logging.getLogger(__name__)
timezone = pytz.timezone("UTC")


@pytest.mark.django_db(reset_sequences=True)
class TestDataIngressPoster:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.collection_name = "telcameras_v2"
        self.URL = f"/ingress/{self.collection_name}/"

        # Create an endpoint
        self.collection_obj = Collection.objects.create(
            name=self.collection_name, consumer_enabled=True
        )

        # Create the sensors in the database
        self.sensor_names = [f"CAM{i}" for i in range(3)]
        for i, sensor_name in enumerate(self.sensor_names):
            Sensors.objects.create(objectnummer=sensor_name, gid=i)

    def add_test_records(self, client, test_days=2):
        # Add records every 5min for multiple days
        Message.objects.all().delete()
        today = date.today()
        start_date = today - timedelta(days=test_days - 1)
        the_dt = datetime(start_date.year, start_date.month, start_date.day)
        while the_dt < (
            datetime(today.year, today.month, today.day) + timedelta(days=1)
        ):
            for sensor_name in self.sensor_names:
                test_post = json.loads(TEST_POST)
                test_post["data"][0]["sensor"] = sensor_name
                test_post["data"][0]["timestamp_start"] = the_dt.isoformat()
                client.post(
                    self.URL,
                    json.dumps(test_post),
                    **AUTHORIZATION_HEADER,
                    content_type="application/json",
                )
            the_dt += timedelta(minutes=5)

        # Then run the parse_ingress script
        parser = TelcameraParser()
        parser.consume(end_at_empty_queue=True)

        return today

    def test_vanilla(self, client):
        today = self.add_test_records(client)

        # Make sure we've got source data
        assert Observation.objects.all().count() > 100

        # Run the aggregator
        call_man_command("complete_aggregate_cmsa15min")

        # Do we have any records in the continuous aggregate table?
        assert Cmsa15Min.objects.all().count() > 500

        # Take a record in the middle of the data in the continuous aggregate table
        # and check whether the record is made up of exactly 3 messages (one every 5 min)
        middle_record = (
            Cmsa15Min.objects.filter(sensor=self.sensor_names[0])
            .filter(timestamp_rounded__gte=(today - timedelta(days=1)).isoformat())
            .order_by("timestamp_rounded")
            .first()
        )
        assert middle_record.basedonxmessages == 3

    def test_aggregate_full_recalculation(self, client):
        today = self.add_test_records(client, test_days=3)

        # Run the aggregator
        call_man_command("complete_aggregate_cmsa15min")

        # Remove the aggregate
        call_man_command("remove_aggregate_cmsa15min")

        assert Cmsa15Min.objects.all().count() == 0

        # Run the aggregator again
        call_man_command("complete_aggregate_cmsa15min")

        # Check the last aggregation record again
        last_record = (
            Cmsa15Min.objects.filter(sensor=self.sensor_names[0])
            .order_by("-timestamp_rounded")
            .first()
        )
        assert today == last_record.timestamp_rounded.date()

    def test_aggregate_recalculation_since_date(self, client):
        today = self.add_test_records(client, test_days=3)

        # Run the aggregator
        call_man_command("complete_aggregate_cmsa15min")

        # Remove the aggregate since yesterday
        a_day_ago = (date.today() - timedelta(days=1)).isoformat()
        call_man_command("remove_aggregate_cmsa15min", f"--since={a_day_ago}")

        # We removed source records from yesterday onwards. That means we now expect the
        # latest date in the continuous aggregate to be the day before yesterday
        last_record = (
            Cmsa15Min.objects.filter(sensor=self.sensor_names[0])
            .order_by("-timestamp_rounded")
            .first()
        )
        assert today - timedelta(days=2) == last_record.timestamp_rounded.date()

        # Run the aggregator again
        call_man_command("complete_aggregate_cmsa15min")

        # Check the last aggregation record again
        last_record = (
            Cmsa15Min.objects.filter(sensor=self.sensor_names[0])
            .order_by("-timestamp_rounded")
            .first()
        )
        assert today == last_record.timestamp_rounded.date()
