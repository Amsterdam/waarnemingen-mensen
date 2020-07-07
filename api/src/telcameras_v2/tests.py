import json
import logging
from datetime import datetime, timedelta
from decimal import Decimal

import pytz
from django.conf import settings
from rest_framework.test import APIClient, APITestCase

from telcameras_v2.models import Observation, CountAggregate, PersonAggregate

log = logging.getLogger(__name__)
timezone = pytz.timezone("UTC")

TEST_POST_SIMPLE = """
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
        }
    ]
}
"""

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
                    ],
                    "personId": "3560f51a-4978-4b82-ab6f-b421e739230d",
                    "speed": 1.5,
                    "record": 5342,
                    "geom": "0.6 1.9,0.7 1.6,0.8 1.3,1 1,1 0.6,1.1 0.3,1.2 -0.1,1.2 -0.3,1.2 -0.8,1.2 -1.2,1.2 -1.6,1.2 -1.9,1 -2.3,0.9 -2.8,0.7 -3.2,0.6 -3.5 (-1.2 -0.9 -0.7 -0.5 -0.2 0.1 0.3 0.5 0.8 1 1.3 1.5 1.8 2.1 2.3 2.5)",
                    "observation_timestamp": "2020-06-12T15:05:00.000Z",
                    "quality": 80
                }, 
                {
                    "distances": [
                        {
                            "personId": "3560f51a-4978-4b82-ab6f-b421e739230d",
                            "observation_timestamp": "2020-06-12T15:05:00.000Z",
                            "distance_to": "0.5 0.6 0.6 0.7 0.7 0.6 0.7 0.6 0.8 0.8 0.8 0.9 0.8 0.6 (-0.7 -0.5 -0.2 0.1 0.3 0.5 0.8 1 1.3 1.5 1.8 2.1 2.3 2.5)"
                        }
                    ],
                    "personId": "653042c8-742a-4735-b619-1f8823304f34",
                    "speed": null,
                    "record": 5343,
                    "geom": "0.4 1.2,0.4 0.7,0.4 0.2,0.5 0,0.7 -0.5,0.5 -1.1,0.6 -1.3,0.5 -2,0.5 -2.2,0.3 -2.8,0.1 -3.3,0.1 -3.6,0.2 -3.9,0.3 -4.3 (-0.5 -0.2 0.1 0.3 0.5 0.8 1 1.3 1.5 1.8 2.1 2.3 2.5 2.8)",
                    "observation_timestamp": "2020-06-12T15:05:00.000Z",
                    "quality": 80
                }
            ]
        }
    ]
}
"""


class DataPosterTest(APITestCase):
    """ Test the second iteration of the api, which receives data in a new format """

    def setUp(self):
        self.URL = '/telcameras/v2/'

    def test_post_new_record(self):
        """ Test posting a new vanilla message """
        post_data = json.loads(TEST_POST_SIMPLE)
        response = self.client.post(
            self.URL,
            post_data,
            **{'HTTP_AUTHORIZATION': f"Token {settings.AUTHORIZATION_TOKEN}"},
            format='json'
        )

        self.assertEqual(response.status_code, 201, response.data)
        self.assertEqual(Observation.objects.all().count(), 1)

        observation = Observation.objects.all()[0]
        fields_to_check = ('sensor', 'sensor_type', 'sensor_state', 'owner', 'supplier', 'purpose', 'latitude',
                           'longitude', 'interval', 'timestamp_message', 'timestamp_start')
        for attr in fields_to_check:
            if type(getattr(observation, attr)) is Decimal:
                self.assertEqual(float(getattr(observation, attr)), post_data[attr])
            elif type(getattr(observation, attr)) is datetime:
                pass
                # TODO: Fix this
                # self.assertEqual(getattr(observation, attr).isoformat(), post_data[attr])
            else:
                self.assertEqual(getattr(observation, attr), post_data[attr])

        self.assertEqual(CountAggregate.objects.all().count(), 1)
        count_aggr = CountAggregate.objects.all()[0]
        for attr in ('type', 'azimuth', 'count_in', 'count_out'):
            # print(post_data['aggregate'][0])
            self.assertEqual(getattr(count_aggr, attr), post_data['aggregate'][0][attr])
        self.assertEqual(count_aggr.external_id, post_data['aggregate'][0]['id'])
        self.assertEqual(count_aggr.message, post_data['message'])
        self.assertEqual(count_aggr.version, post_data['version'])

        # self.assertEqual(observation.sensor, post_data['sensor'])
    #     self.assertEqual(sensor.sensor_code, post_data['sensor'])
    #
    # def test_post_fails_without_token(self):
    #     response = self.client.post(self.URL, json.loads(TEST_POST), format='json')
    #     self.assertEqual(response.status_code, 401)
    #     self.assertEqual(Sensor.objects.all().count(), 0)
    #
    # def test_not_creating_two_same_sensors(self):
    #     self.client.post(self.URL, json.loads(TEST_POST),
    #                      **{'HTTP_AUTHORIZATION': f"Token {settings.AUTHORIZATION_TOKEN}"}, format='json')
    #     self.client.post(self.URL, json.loads(TEST_POST),
    #                      **{'HTTP_AUTHORIZATION': f"Token {settings.AUTHORIZATION_TOKEN}"}, format='json')
    #     self.assertEqual(Sensor.objects.all().count(), 1)  # ONLY ONE SENSOR IS CREATED
    #     self.assertEqual(ObservationAggregate.objects.all().count(), 4)
    #     self.assertEqual(PersonObservation.objects.all().count(), 28)
    #
    # def test_creating_a_new_sensors_with_other_sensor_code(self):
    #     post_data = json.loads(TEST_POST)
    #     self.client.post(self.URL, post_data,
    #                      **{'HTTP_AUTHORIZATION': f"Token {settings.AUTHORIZATION_TOKEN}"}, format='json')
    #     post_data['sensor'] = "OTHER_SENSOR_NAME"
    #     self.client.post(self.URL, post_data,
    #                      **{'HTTP_AUTHORIZATION': f"Token {settings.AUTHORIZATION_TOKEN}"}, format='json')
    #     self.assertEqual(Sensor.objects.all().count(), 2)  # TWO SENSORS CREATED
    #     self.assertEqual(ObservationAggregate.objects.all().count(), 4)
    #     self.assertEqual(PersonObservation.objects.all().count(), 28)
    #
    #     new_sensor = Sensor.objects.all().order_by('-id')[0]
    #     self.assertEqual(new_sensor.sensor_code, post_data['sensor'])
    #
    # def test_creating_a_new_sensors_with_other_sensor_type(self):
    #     post_data = json.loads(TEST_POST)
    #     self.client.post(self.URL, post_data,
    #                      **{'HTTP_AUTHORIZATION': f"Token {settings.AUTHORIZATION_TOKEN}"}, format='json')
    #     post_data['sensor_type'] = "OTHER_SENSOR_TYPE"
    #     self.client.post(self.URL, post_data,
    #                      **{'HTTP_AUTHORIZATION': f"Token {settings.AUTHORIZATION_TOKEN}"}, format='json')
    #     self.assertEqual(Sensor.objects.all().count(), 2)  # TWO SENSORS CREATED
    #     self.assertEqual(ObservationAggregate.objects.all().count(), 4)
    #     self.assertEqual(PersonObservation.objects.all().count(), 28)
    #
    #     new_sensor = Sensor.objects.all().order_by('-id')[0]
    #     self.assertEqual(new_sensor.sensor_type, post_data['sensor_type'])
    #
    # def test_creating_a_new_sensors_with_other_latitude(self):
    #     post_data = json.loads(TEST_POST)
    #     self.client.post(self.URL, post_data,
    #                      **{'HTTP_AUTHORIZATION': f"Token {settings.AUTHORIZATION_TOKEN}"}, format='json')
    #     post_data['latitude'] = 1.23456789
    #     self.client.post(self.URL, post_data,
    #                      **{'HTTP_AUTHORIZATION': f"Token {settings.AUTHORIZATION_TOKEN}"}, format='json')
    #     self.assertEqual(Sensor.objects.all().count(), 2)  # TWO SENSORS CREATED
    #     self.assertEqual(ObservationAggregate.objects.all().count(), 4)
    #     self.assertEqual(PersonObservation.objects.all().count(), 28)
    #
    #     new_sensor = Sensor.objects.all().order_by('-id')[0]
    #     self.assertEqual(new_sensor.latitude, post_data['latitude'])
    #
    # def test_creating_a_new_sensors_with_other_longitude(self):
    #     post_data = json.loads(TEST_POST)
    #     self.client.post(self.URL, post_data,
    #                      **{'HTTP_AUTHORIZATION': f"Token {settings.AUTHORIZATION_TOKEN}"}, format='json')
    #     post_data['longitude'] = 1.23456789
    #     self.client.post(self.URL, post_data,
    #                      **{'HTTP_AUTHORIZATION': f"Token {settings.AUTHORIZATION_TOKEN}"}, format='json')
    #     self.assertEqual(Sensor.objects.all().count(), 2)  # TWO SENSORS CREATED
    #     self.assertEqual(ObservationAggregate.objects.all().count(), 4)
    #     self.assertEqual(PersonObservation.objects.all().count(), 28)
    #
    #     new_sensor = Sensor.objects.all().order_by('-id')[0]
    #     self.assertEqual(new_sensor.longitude, post_data['longitude'])
    #
    # def test_creating_a_new_sensors_with_other_interval(self):
    #     post_data = json.loads(TEST_POST)
    #     self.client.post(self.URL, post_data,
    #                      **{'HTTP_AUTHORIZATION': f"Token {settings.AUTHORIZATION_TOKEN}"}, format='json')
    #     post_data['interval'] = 120
    #     self.client.post(self.URL, post_data,
    #                      **{'HTTP_AUTHORIZATION': f"Token {settings.AUTHORIZATION_TOKEN}"}, format='json')
    #     self.assertEqual(Sensor.objects.all().count(), 2)  # TWO SENSORS CREATED
    #     self.assertEqual(ObservationAggregate.objects.all().count(), 4)
    #     self.assertEqual(PersonObservation.objects.all().count(), 28)
    #
    #     new_sensor = Sensor.objects.all().order_by('-id')[0]
    #     self.assertEqual(new_sensor.interval, post_data['interval'])
    #
    # def test_creating_a_new_sensors_with_other_version(self):
    #     post_data = json.loads(TEST_POST)
    #     self.client.post(self.URL, post_data,
    #                      **{'HTTP_AUTHORIZATION': f"Token {settings.AUTHORIZATION_TOKEN}"}, format='json')
    #     post_data['version'] = '1.2.3.4'
    #     self.client.post(self.URL, post_data,
    #                      **{'HTTP_AUTHORIZATION': f"Token {settings.AUTHORIZATION_TOKEN}"}, format='json')
    #     self.assertEqual(Sensor.objects.all().count(), 2)  # TWO SENSORS CREATED
    #     self.assertEqual(ObservationAggregate.objects.all().count(), 4)
    #     self.assertEqual(PersonObservation.objects.all().count(), 28)
    #
    #     new_sensor = Sensor.objects.all().order_by('-id')[0]
    #     self.assertEqual(new_sensor.version, post_data['version'])
    #
    # def test_newly_created_sensor_copies_over_all_other_details(self):
    #     post_data = json.loads(TEST_POST)
    #     self.client.post(self.URL, post_data,
    #                      **{'HTTP_AUTHORIZATION': f"Token {settings.AUTHORIZATION_TOKEN}"}, format='json')
    #
    #     # Add some data into the other fields
    #     sensor = Sensor.objects.all().order_by('-id')[0]
    #     detail_fields = {
    #         'owner': "Willen van Oranje",
    #         'supplier': "Witte Corneliszoon de With",
    #         'purpose': 'Something',
    #         'area_gross': 123,
    #         'area_net': 120,
    #         'width': 10,
    #         'length': 20,
    #         'valid_from': datetime.now(pytz.UTC),
    #         'valid_until': datetime.now(pytz.UTC) + timedelta(days=5)
    #     }
    #     for k, v in detail_fields.items():
    #         setattr(sensor, k, v)
    #     sensor.save()
    #
    #     post_data['version'] = '1.2.3.4'
    #     self.client.post(self.URL, post_data,
    #                      **{'HTTP_AUTHORIZATION': f"Token {settings.AUTHORIZATION_TOKEN}"}, format='json')
    #     self.assertEqual(Sensor.objects.all().count(), 2)  # TWO SENSORS CREATED
    #     self.assertEqual(ObservationAggregate.objects.all().count(), 4)
    #     self.assertEqual(PersonObservation.objects.all().count(), 28)
    #     new_sensor = Sensor.objects.all().order_by('-id')[0]
    #
    #     # Check whether the new version is correctly different in the new sensor
    #     self.assertEqual(new_sensor.version, post_data['version'])
    #
    #     # Check whether the basic things are taken correctly from the new validated data
    #     self.assertEqual(new_sensor.sensor_code, post_data['sensor'])
    #     for attr in ('sensor_type', 'latitude', 'longitude', 'interval'):
    #         self.assertEqual(getattr(new_sensor, attr), post_data[attr])
    #
    #     # Check whether the details which are not in the POSTed json are copied over
    #     # correctly from the previously last created sensor
    #     for k, v in detail_fields.items():
    #         self.assertEqual(getattr(new_sensor, k), detail_fields[k])
    #
    # def test_empty_aggregates_are_also_saved(self):
    #     post_data = json.loads(TEST_POST)
    #     # Add an empty direction dict to see if that is also stored
    #     post_data['direction'].append({
    #         "azimuth": 115,
    #         "count": 0,
    #         "cumulative_distance": 0,
    #         "cumulative_time": 0,
    #         "median_speed": 0,
    #         "signals": []}
    #     )
    #     response = self.client.post(self.URL, post_data,
    #                                 **{'HTTP_AUTHORIZATION': f"Token {settings.AUTHORIZATION_TOKEN}"}, format='json')
    #     self.assertEqual(response.status_code, 201, response.data)
    #     self.assertEqual(Sensor.objects.all().count(), 1)
    #     self.assertEqual(ObservationAggregate.objects.all().count(), 3)
    #     self.assertEqual(PersonObservation.objects.all().count(), 14)
    #
    # def test_405_on_get(self):
    #     response = self.client.get(self.URL,
    #                                **{'HTTP_AUTHORIZATION': f"Token {settings.AUTHORIZATION_TOKEN}"}, format='json')
    #     self.assertEqual(response.status_code, 405, response.data)
    #
    # def test_405_on_put(self):
    #     response = self.client.put(self.URL, json.loads(TEST_POST),
    #                                **{'HTTP_AUTHORIZATION': f"Token {settings.AUTHORIZATION_TOKEN}"}, format='json')
    #     self.assertEqual(response.status_code, 405, response.data)
    #
    # def test_405_on_patch(self):
    #     response = self.client.patch(self.URL, json.loads(TEST_POST),
    #                                  **{'HTTP_AUTHORIZATION': f"Token {settings.AUTHORIZATION_TOKEN}"}, format='json')
    #     self.assertEqual(response.status_code, 405, response.data)
    #
    # def test_405_on_delete(self):
    #     response = self.client.delete(self.URL, json.loads(TEST_POST),
    #                                   **{'HTTP_AUTHORIZATION': f"Token {settings.AUTHORIZATION_TOKEN}"}, format='json')
    #     self.assertEqual(response.status_code, 405, response.data)
