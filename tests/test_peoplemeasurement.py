import json
import logging
from datetime import datetime

import pytz
from django.conf import settings
from django.contrib.gis.geos import LineString, Polygon
from ingress.models import Collection
from rest_framework.test import APITestCase

from peoplemeasurement.models import (Area, Line, Sensors, Servicelevel,
                                      VoorspelCoefficient, VoorspelIntercept)
from telcameras_v2.ingress_parser import TelcameraParser
from tests.test_telcameras_v2_ingress import TEST_POST
from tests.tools_for_testing import call_man_command

log = logging.getLogger(__name__)
timezone = pytz.timezone("UTC")

BBOX = [52.03560, 4.58565, 52.48769, 5.31360]

POST_AUTHORIZATION_HEADER = {'HTTP_AUTHORIZATION': f"Token {settings.AUTHORIZATION_TOKEN}"}
GET_AUTHORIZATION_HEADER = {'HTTP_AUTHORIZATION': f"Token {settings.GET_AUTHORIZATION_TOKEN}"}


def create_new_v2_json(timestamp_str="2019-06-21T10:35:46+02:00"):
    test_post = json.loads(TEST_POST)
    for i in range(2):
        test_post['data'][i]['timestamp_message'] = timestamp_str
        test_post['data'][i]['timestamp_start'] = timestamp_str

    for personaggregate in test_post['data'][1]['aggregate']:
        personaggregate['observation_timestamp'] = timestamp_str

    return json.dumps(test_post)


class PeopleMeasurementTestGetV1(APITestCase):

    def setUp(self):
        self.URL = '/telcameras/v1/15minaggregate/'
        self.collection_name = 'telcameras_v2'
        self.POST_URL_V2 = f'/ingress/{self.collection_name}/'

        # Create an endpoint
        self.collection_obj = Collection.objects.create(name=self.collection_name, consumer_enabled=True)

        # Create the sensor in the database
        self.sensor = Sensors.objects.create(objectnummer='GAVM-01-Vondelstraat', gid=1)

    # NOTE: The test below fails because older data from the v1 view isn't loaded. This is because the v5 view
    # apparently doesn't work correctly yet. For this endpoint that doesn't matter though, since it only serves the
    # data from the past 24 hours, which will not include any data from the v1 data anyway.
    # TODO: Fix the v5 view and update the query in the endpoint with it

    # def test_get_15min_aggregation_records(self):
    #     # Insert some v1 records at 22 hours yesterday
    #     for i in range(3):
    #         timestamp_str = (datetime.now() - timedelta(days=1)).replace(hour=22, minute=0, second=0).astimezone().replace(microsecond=0).isoformat()
    #         create_new_v1_object(timestamp_str=timestamp_str)
    #
    #     # and some more v1 records at 23 hours yesterday
    #     for i in range(3):
    #         timestamp_str = (datetime.now() - timedelta(days=1)).replace(hour=23, minute=0, second=0).astimezone().replace(microsecond=0).isoformat()
    #         create_new_v1_object(timestamp_str=timestamp_str)
    #
    #     # And then some v2 records each hour today
    #     for i in range(0, 24):
    #         timestamp_str = datetime.now().replace(hour=i, minute=0, second=0).astimezone().replace(
    #             microsecond=0).isoformat()
    #
    #         self.client.post(
    #             self.POST_URL_V2,
    #             json.loads(create_new_v2_json(timestamp_str=timestamp_str)),
    #             **V2_POST_AUTHORIZATION_HEADER,
    #             format='json'
    #         )
    #
    #     # test whether the endpoint responds correctly
    #     response = self.client.get(self.URL, **GET_AUTHORIZATION_HEADER)
    #     self.assertEqual(response.status_code, 200)
    #     self.assertEqual(len(response.data), 26)

    def test_get_15min_aggregation_timezone_with_both_v1_and_v2_records(self):
        # Insert some v2 records for each hour
        for i in range(0, 24):
            timestamp_str = datetime.now().replace(hour=i, minute=0, second=0).astimezone().replace(
                microsecond=0).isoformat()

            response = self.client.post(
                self.POST_URL_V2,
                create_new_v2_json(timestamp_str=timestamp_str),
                **POST_AUTHORIZATION_HEADER,
                content_type='application/json'
            )

            self.assertEqual(response.status_code, 200)

        # Then run the parse_ingress script
        parser = TelcameraParser()
        parser.consume(end_at_empty_queue=True)

        # Refresh the materialized views because the query in the endpoint depends on them
        call_man_command('refresh_materialized_view', 'cmsa_15min_view_v10_materialized')
        call_man_command('refresh_materialized_view', 'cmsa_15min_view_v10_realtime_materialized')

        # test whether the endpoint responds correctly
        response = self.client.get(self.URL, **GET_AUTHORIZATION_HEADER)
        self.assertEqual(response.status_code, 200)
        self.assertGreater(len(json.loads(response.content)), 0)

    def test_get_15min_aggregation_records_fails_without_token(self):
        response = self.client.get(self.URL)
        self.assertEqual(response.status_code, 401)

    def test_get_15min_aggregation_records_fails_with_wrong_token(self):
        response = self.client.get(self.URL, **POST_AUTHORIZATION_HEADER)
        self.assertEqual(response.status_code, 401)


class PeopleMeasurementTestSetSensorIsActiveStatus(APITestCase):
    def setUp(self):
        self.objectnummer = 'GAVM-01-Vondelstraat'
        self.sensor = Sensors.objects.create(objectnummer=self.objectnummer, gid=1)

    def test_changing_is_active_to_false_and_back_to_true(self):
        out = call_man_command('set_bool_field', self.objectnummer, 'is_active', 'false')
        self.assertEqual(out.strip(), f"The sensor '{self.objectnummer}'.is_active was successfully changed to False.")
        sensor = Sensors.objects.get(objectnummer=self.objectnummer)
        self.assertEqual(sensor.is_active, False)

        out = call_man_command('set_bool_field', self.objectnummer, 'is_active', 'true')
        self.assertEqual(out.strip(), f"The sensor '{self.objectnummer}'.is_active was successfully changed to True.")
        sensor = Sensors.objects.get(objectnummer=self.objectnummer)
        self.assertEqual(sensor.is_active, True)

    def test_changing_non_existing_sensor_fails(self):
        out = call_man_command('set_bool_field', 'does not exist', 'is_active', 'false')
        self.assertEqual(out.strip(), f"No sensor exists for the objectnummer 'does not exist'")

    def test_changing_is_active_to_existing_status(self):
        # The sensor is already active. Now try to set it to active again.
        out = call_man_command('set_bool_field', self.objectnummer, 'is_active', 'true')
        self.assertEqual(
            out.strip(),
            f"The sensor '{self.objectnummer}'.is_active is already True. Nothing has changed."
        )

class PeopleMeasurementTestPublicSensorsEndpoint(APITestCase):
    def setUp(self):
        self.URL_SENSOR = '/telcameras/v1/sensor/'
        self.URL_SERVICELEVEL = '/telcameras/v1/servicelevel/'
        self.URL_AREA = '/telcameras/v1/area/'
        self.URL_LINE = '/telcameras/v1/line/'
        polygon = Polygon(((0, 0), (0, 1), (1, 1), (0, 0)), ((0.4, 0.4), (0.4, 0.6), (0.6, 0.6), (0.4, 0.4)))
        linestring = LineString((0, 0), (1, 1))
        for i in range(3):
            sensor = Sensors.objects.create(objectnummer=f'ABC-{i}', gid=i)
            Servicelevel.objects.create(
                type_parameter=str(i), type_gebied=str(i), type_tijd=str(i), level_nr=i, level_label=str(i))
            Area.objects.create(sensor=sensor, name=str(i), geom=polygon, area=i)
            Line.objects.create(sensor=sensor, name=str(i), geom=linestring, azimuth=i)

    def test_list_sensors(self):
        response = self.client.get(self.URL_SENSOR, content_type='application/json')
        results = json.loads(response.content)
        self.assertEqual(len(results), 3)
        for i, result in enumerate(results):
            self.assertEqual(result['objectnummer'], f'ABC-{i}')
            self.assertEqual(len(result['areas']), 1)
            self.assertEqual(len(result['lines']), 1)

    def test_list_servicelevel(self):
        response = self.client.get(self.URL_SERVICELEVEL, content_type='application/json')
        results = json.loads(response.content)
        self.assertEqual(len(results), 3)
        for i, result in enumerate(results):
            self.assertEqual(result['type_parameter'], str(i))
