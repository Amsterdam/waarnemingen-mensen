import json
import logging
from datetime import date, datetime, timedelta

import pytest
import pytz
from ingress.models import Collection, Message

from peoplemeasurement.models import Sensors
from telcameras_v2.ingress_parser import TelcameraParser
from tests.test_telcameras_v2_ingress import AUTHORIZATION_HEADER, TEST_POST
from tests.tools_for_testing import call_man_command

log = logging.getLogger(__name__)
timezone = pytz.timezone("UTC")


@pytest.mark.django_db
class TestDataIngressPoster:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.collection_name = 'telcameras_v2'
        self.URL = f'/ingress/{self.collection_name}/'

        # Create an endpoint
        self.collection_obj = Collection.objects.create(name=self.collection_name, consumer_enabled=True)

        # Create the sensors in the database
        self.sensor_names = [f'CAM{i}' for i in range(3)]
        for i, sensor_name in enumerate(self.sensor_names):
            Sensors.objects.create(objectnummer=sensor_name, gid=i)

    def test_vanilla(self, client):
        # Add records every 5min for multiple days
        Message.objects.all().delete()
        test_days = 2
        today = date.today()
        start_date = today - timedelta(days=test_days)
        the_dt = datetime(start_date.year, start_date.month, start_date.day)
        while the_dt < datetime(today.year, today.month, today.day):
            # print(the_dt)
            for sensor_name in self.sensor_names:
                # print(f'    {sensor_name}')
                test_post = json.loads(TEST_POST)
                test_post['data'][0]['sensor'] = sensor_name
                client.post(self.URL, TEST_POST, **AUTHORIZATION_HEADER, content_type='application/json')
            the_dt += timedelta(minutes=15)

        # Then run the parse_ingress script
        parser = TelcameraParser()
        parser.consume(end_at_empty_queue=True)

        # Run the aggregator
        call_man_command('complete_aggregate', 'continuousaggregate_cmsa15min')

        # TODO: Check whether the continuous aggragation table contains the correct records
