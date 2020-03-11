import json
import logging
from datetime import datetime, timedelta

import pytz
from rest_framework.test import APITestCase

from telcameras_v2.models import ObservationAggregate, PersonObservation, Sensor

log = logging.getLogger(__name__)
timezone = pytz.timezone("UTC")

TEST_POST = """
{
    "latitude": -4.07266993567023,
    "sensor": "CMSA-GAWW-17",
    "interval": 60,
    "id": 26328123,
    "sensor_type": "counting_camera",
    "version": "2.0.0.2",
    "longitude": -8.14533987134047,
    "timestamp": "2020-01-22T11:03:00+01:00",
    "direction": [
        {
            "azimuth": 222,
            "count": 5,
            "cumulative_distance": 24.202545166015625,
            "cumulative_time": 36,
            "median_speed": 0.6534567475318909,
            "signals": [
                {
                    "record": "60669993-78bf-4d86-9bd5-74c60d0ab42f",
                    "distance": 4.881822109222412,
                    "time": 10,
                    "speed": 0.5919221639633179,
                    "observation_timestamp": "2020-01-22T11:03:21+01:00",
                    "type": "pedestrian"
                },
                {
                    "record": "23408de5-7e40-44f8-8550-5238aa15c1cc",
                    "distance": 4.951663970947266,
                    "time": 9,
                    "speed": 0.5749661922454834,
                    "observation_timestamp": "2020-01-22T11:03:22+01:00",
                    "type": "pedestrian"
                },
                {
                    "record": "586e9a1e-ea78-41f1-a722-e9bb6fd3e28f",
                    "distance": 4.884674549102783,
                    "time": 6,
                    "speed": 0.8311007618904114,
                    "observation_timestamp": "2020-01-22T11:03:25+01:00",
                    "type": "pedestrian"
                },
                {
                    "record": "334e7188-f836-4305-9088-4a6cc2a6e5fe",
                    "distance": 4.765134334564209,
                    "time": 8,
                    "speed": 0.6534567475318909,
                    "observation_timestamp": "2020-01-22T11:03:33+01:00",
                    "type": "pedestrian"
                },
                {
                    "record": "2c466581-3248-4cb0-bffe-c80f5b07cdbd",
                    "distance": 4.7192487716674805,
                    "time": 3,
                    "speed": 1.6591111421585083,
                    "observation_timestamp": "2020-01-22T11:03:49+01:00",
                    "type": "cyclist"
                }
            ]
        },
        {
            "azimuth": 111,
            "count": 9,
            "cumulative_distance": 37.50764846801758,
            "cumulative_time": 89,
            "median_speed": 0.6487004160881042,
            "signals": [
                {
                    "record": "8146dea1-09c5-44fa-909d-5fc082fb6739",
                    "distance": 4.495092868804932,
                    "time": 8,
                    "speed": 0.6732875108718872,
                    "observation_timestamp": "2020-01-22T11:03:13+01:00",
                    "type": "pedestrian"
                },
                {
                    "record": "f9173448-ecbf-49f0-a961-434da3820732",
                    "distance": 4.995566368103027,
                    "time": 9,
                    "speed": 0.7796541452407837,
                    "observation_timestamp": "2020-01-22T11:03:14+01:00",
                    "type": "pedestrian"
                },
                {
                    "record": "4ed11c4d-257e-41e6-b74b-dfe3939284b1",
                    "distance": 4.87014627456665,
                    "time": 8,
                    "speed": 0.6236875653266907,
                    "observation_timestamp": "2020-01-22T11:03:15+01:00",
                    "type": "pedestrian"
                },
                {
                    "record": "a2e2576f-5104-4f77-9021-96699b108e3f",
                    "distance": 2.0701537132263184,
                    "time": 4,
                    "speed": 0.6487004160881042,
                    "observation_timestamp": "2020-01-22T11:03:31+01:00",
                    "type": "pedestrian"
                },
                {
                    "record": "e17588ae-50eb-4664-86d8-a1492f375c9c",
                    "distance": 4.845158100128174,
                    "time": 10,
                    "speed": 0.7116493582725525,
                    "observation_timestamp": "2020-01-22T11:03:35+01:00",
                    "type": "pedestrian"
                },
                {
                    "record": "e13d3244-d99b-408b-8cee-8a6ce23b90a7",
                    "distance": 4.769189834594727,
                    "time": 10,
                    "speed": 0.879210889339447,
                    "observation_timestamp": "2020-01-22T11:03:36+01:00",
                    "type": "pedestrian"
                },
                {
                    "record": "caad8fad-de00-46a6-ab51-7380e32c4d96",
                    "distance": 2.1835408210754395,
                    "time": 5,
                    "speed": 0.540094792842865,
                    "observation_timestamp": "2020-01-22T11:03:38+01:00",
                    "type": "pedestrian"
                },
                {
                    "record": "c9c023d6-2e80-4fea-aebd-9ca3213a220e",
                    "distance": 4.5384135246276855,
                    "time": 10,
                    "speed": 0.6150989532470703,
                    "observation_timestamp": "2020-01-22T11:03:39+01:00",
                    "type": "pedestrian"
                },
                {
                    "record": "fa139ebb-2c00-47db-8c14-c570c6718d6e",
                    "distance": 4.740387916564941,
                    "time": 25,
                    "speed": 0.4346027970314026,
                    "observation_timestamp": "2020-01-22T11:03:44+01:00",
                    "type": "cyclist"
                }
            ]
        }
    ]
}
"""


class DataPosterTest(APITestCase):
    """ Test the second iteration of the api, which receives data from the company "hig" """

    def setUp(self):
        self.URL = '/telcameras/v2/'

    def test_post_new_record(self):
        """ Test posting a new vanilla message """
        post_data = json.loads(TEST_POST)
        response = self.client.post(self.URL, post_data, format='json')
        self.assertEqual(response.status_code, 201, response.data)
        self.assertEqual(Sensor.objects.all().count(), 1)
        self.assertEqual(ObservationAggregate.objects.all().count(), 2)
        self.assertEqual(PersonObservation.objects.all().count(), 14)

        sensor = Sensor.objects.all()[0]
        self.assertEqual(sensor.sensor_code, post_data['sensor'])
        for attr in ('sensor_type', 'latitude', 'longitude', 'interval', 'version'):
            self.assertEqual(getattr(sensor, attr), post_data[attr])

    def test_not_creating_two_same_sensors(self):
        self.client.post(self.URL, json.loads(TEST_POST), format='json')
        self.client.post(self.URL, json.loads(TEST_POST), format='json')
        self.assertEqual(Sensor.objects.all().count(), 1)  # ONLY ONE SENSOR IS CREATED
        self.assertEqual(ObservationAggregate.objects.all().count(), 4)
        self.assertEqual(PersonObservation.objects.all().count(), 28)

    def test_creating_a_new_sensors_with_other_sensor_code(self):
        post_data = json.loads(TEST_POST)
        self.client.post(self.URL, post_data, format='json')
        post_data['sensor'] = "OTHER_SENSOR_NAME"
        self.client.post(self.URL, post_data, format='json')
        self.assertEqual(Sensor.objects.all().count(), 2)  # TWO SENSORS CREATED
        self.assertEqual(ObservationAggregate.objects.all().count(), 4)
        self.assertEqual(PersonObservation.objects.all().count(), 28)

        new_sensor = Sensor.objects.all().order_by('-id')[0]
        self.assertEqual(new_sensor.sensor_code, post_data['sensor'])

    def test_creating_a_new_sensors_with_other_sensor_type(self):
        post_data = json.loads(TEST_POST)
        self.client.post(self.URL, post_data, format='json')
        post_data['sensor_type'] = "OTHER_SENSOR_TYPE"
        self.client.post(self.URL, post_data, format='json')
        self.assertEqual(Sensor.objects.all().count(), 2)  # TWO SENSORS CREATED
        self.assertEqual(ObservationAggregate.objects.all().count(), 4)
        self.assertEqual(PersonObservation.objects.all().count(), 28)

        new_sensor = Sensor.objects.all().order_by('-id')[0]
        self.assertEqual(new_sensor.sensor_type, post_data['sensor_type'])

    def test_creating_a_new_sensors_with_other_latitude(self):
        post_data = json.loads(TEST_POST)
        self.client.post(self.URL, post_data, format='json')
        post_data['latitude'] = 1.23456789
        self.client.post(self.URL, post_data, format='json')
        self.assertEqual(Sensor.objects.all().count(), 2)  # TWO SENSORS CREATED
        self.assertEqual(ObservationAggregate.objects.all().count(), 4)
        self.assertEqual(PersonObservation.objects.all().count(), 28)

        new_sensor = Sensor.objects.all().order_by('-id')[0]
        self.assertEqual(new_sensor.latitude, post_data['latitude'])

    def test_creating_a_new_sensors_with_other_longitude(self):
        post_data = json.loads(TEST_POST)
        self.client.post(self.URL, post_data, format='json')
        post_data['longitude'] = 1.23456789
        self.client.post(self.URL, post_data, format='json')
        self.assertEqual(Sensor.objects.all().count(), 2)  # TWO SENSORS CREATED
        self.assertEqual(ObservationAggregate.objects.all().count(), 4)
        self.assertEqual(PersonObservation.objects.all().count(), 28)

        new_sensor = Sensor.objects.all().order_by('-id')[0]
        self.assertEqual(new_sensor.longitude, post_data['longitude'])

    def test_creating_a_new_sensors_with_other_interval(self):
        post_data = json.loads(TEST_POST)
        self.client.post(self.URL, post_data, format='json')
        post_data['interval'] = 120
        self.client.post(self.URL, post_data, format='json')
        self.assertEqual(Sensor.objects.all().count(), 2)  # TWO SENSORS CREATED
        self.assertEqual(ObservationAggregate.objects.all().count(), 4)
        self.assertEqual(PersonObservation.objects.all().count(), 28)

        new_sensor = Sensor.objects.all().order_by('-id')[0]
        self.assertEqual(new_sensor.interval, post_data['interval'])

    def test_creating_a_new_sensors_with_other_version(self):
        post_data = json.loads(TEST_POST)
        self.client.post(self.URL, post_data, format='json')
        post_data['version'] = '1.2.3.4'
        self.client.post(self.URL, post_data, format='json')
        self.assertEqual(Sensor.objects.all().count(), 2)  # TWO SENSORS CREATED
        self.assertEqual(ObservationAggregate.objects.all().count(), 4)
        self.assertEqual(PersonObservation.objects.all().count(), 28)

        new_sensor = Sensor.objects.all().order_by('-id')[0]
        self.assertEqual(new_sensor.version, post_data['version'])

    def test_newly_created_sensor_copies_over_all_other_details(self):
        post_data = json.loads(TEST_POST)
        self.client.post(self.URL, post_data, format='json')

        # Add some data into the other fields
        sensor = Sensor.objects.all().order_by('-id')[0]
        detail_fields = {
            'owner': "Willen van Oranje",
            'supplier': "Witte Corneliszoon de With",
            'purpose': 'Something',
            'area_gross': 123,
            'area_net': 120,
            'width': 10,
            'length': 20,
            'valid_from': datetime.now(pytz.UTC),
            'valid_until': datetime.now(pytz.UTC) + timedelta(days=5)
        }
        for k, v in detail_fields.items():
            setattr(sensor, k, v)
        sensor.save()

        post_data['version'] = '1.2.3.4'
        self.client.post(self.URL, post_data, format='json')
        self.assertEqual(Sensor.objects.all().count(), 2)  # TWO SENSORS CREATED
        self.assertEqual(ObservationAggregate.objects.all().count(), 4)
        self.assertEqual(PersonObservation.objects.all().count(), 28)
        new_sensor = Sensor.objects.all().order_by('-id')[0]

        # Check whether the new version is correctly different in the new sensor
        self.assertEqual(new_sensor.version, post_data['version'])

        # Check whether the basic things are taken correctly from the new validated data
        self.assertEqual(new_sensor.sensor_code, post_data['sensor'])
        for attr in ('sensor_type', 'latitude', 'longitude', 'interval'):
            self.assertEqual(getattr(new_sensor, attr), post_data[attr])

        # Check whether the details which are not in the POSTed json are copied over
        # correctly from the previously last created sensor
        for k, v in detail_fields.items():
            self.assertEqual(getattr(new_sensor, k), detail_fields[k])

    def test_empty_aggregates_are_also_saved(self):
        post_data = json.loads(TEST_POST)
        # Add an empty direction dict to see if that is also stored
        post_data['direction'].append({
            "azimuth": 115,
            "count": 0,
            "cumulative_distance": 0,
            "cumulative_time": 0,
            "median_speed": 0,
            "signals": []}
        )
        response = self.client.post(self.URL, post_data, format='json')
        self.assertEqual(response.status_code, 201, response.data)
        self.assertEqual(Sensor.objects.all().count(), 1)
        self.assertEqual(ObservationAggregate.objects.all().count(), 3)
        self.assertEqual(PersonObservation.objects.all().count(), 14)

    def test_405_on_get(self):
        response = self.client.get(self.URL, format='json')
        self.assertEqual(response.status_code, 405, response.data)

    def test_405_on_put(self):
        response = self.client.put(self.URL, json.loads(TEST_POST), format='json')
        self.assertEqual(response.status_code, 405, response.data)

    def test_405_on_patch(self):
        response = self.client.patch(self.URL, json.loads(TEST_POST), format='json')
        self.assertEqual(response.status_code, 405, response.data)

    def test_405_on_delete(self):
        response = self.client.delete(self.URL, json.loads(TEST_POST), format='json')
        self.assertEqual(response.status_code, 405, response.data)
