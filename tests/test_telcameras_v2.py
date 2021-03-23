import json
import logging
from datetime import datetime
from decimal import Decimal

import pytz
from dateutil import parser
from django.conf import settings
from django.test import TestCase, override_settings
from model_bakery import baker
from rest_framework.test import APITestCase

from peoplemeasurement.models import Sensors
from telcameras_v2.models import CountAggregate, Observation, PersonAggregate
from telcameras_v2.tools import scramble_count_aggregate

log = logging.getLogger(__name__)
timezone = pytz.timezone("UTC")

AUTHORIZATION_HEADER = {'HTTP_AUTHORIZATION': f"Token {settings.AUTHORIZATION_TOKEN}"}

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


def create_new_v2_json(timestamp_str="2019-06-21T10:35:46+02:00"):
    test_post = json.loads(TEST_POST)
    for i in range(2):
        test_post['data'][i]['timestamp_message'] = timestamp_str
        test_post['data'][i]['timestamp_start'] = timestamp_str

    for personaggregate in test_post['data'][1]['aggregate']:
        personaggregate['observation_timestamp'] = timestamp_str

    return json.dumps(test_post)


class DataPosterTest(APITestCase):
    """ Test the second iteration of the api, which receives data in a new format """

    def setUp(self):
        self.URL = '/telcameras/v2/'
        self.sensor = Sensors.objects.create(objectnummer='GAVM-01-Vondelstraat')

    @override_settings(STORE_ALL_DATA_TELCAMERAS_V2=False)
    def test_post_is_not_saved_with_non_existing_sensor(self):
        post_data = json.loads(TEST_POST)
        post_data['data'][0]['sensor'] = 'does not exist'
        response = self.client.post(self.URL, post_data, **AUTHORIZATION_HEADER, format='json')
        self.assertEqual(response.status_code, 200)  # We get a 200, but no data is added to the DB. This is as designed
        self.assertEqual(response.content, b'"The sensor \'does not exist\' was not found, so the data is not stored."')
        self.assertEqual(Observation.objects.count(), 0)

    @override_settings(STORE_ALL_DATA_TELCAMERAS_V2=False)
    def test_post_is_not_saved_with_inactive_sensor(self):
        # Set the sensor to inactive
        self.sensor.is_active = False
        self.sensor.save()

        # Test the response
        post_data = json.loads(TEST_POST)
        response = self.client.post(self.URL, post_data, **AUTHORIZATION_HEADER, format='json')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, b'"The sensor \'GAVM-01-Vondelstraat\' exists but is not active."')
        self.assertEqual(Observation.objects.count(), 0)

        # Set the sensor back to active again
        self.sensor.is_active = True
        self.sensor.save()

    @override_settings(STORE_ALL_DATA_TELCAMERAS_V2=True)  # It is by default true, but to make it explicit I also override it here
    def test_post_is_saved_with_non_existing_sensor_if_STORE_ALL_DATA_is_true(self):
        post_data = json.loads(TEST_POST)
        post_data['data'][0]['sensor'] = 'does not exist'
        response = self.client.post(self.URL, post_data, **AUTHORIZATION_HEADER, format='json')
        self.assertEqual(response.status_code, 201)  # We get a 200, but no data is added to the DB. This is as designed
        self.assertEqual(Observation.objects.count(), 1)

    @override_settings(STORE_ALL_DATA_TELCAMERAS_V2=True)  # It is by default true, but to make it explicit I also override it here
    def test_post_is_not_saved_with_inactive_sensor_if_STORE_ALL_DATA_is_true(self):
        # Set the sensor to inactive
        self.sensor.is_active = False
        self.sensor.save()

        # Test the response
        post_data = json.loads(TEST_POST)
        response = self.client.post(self.URL, post_data, **AUTHORIZATION_HEADER, format='json')
        self.assertEqual(response.status_code, 201)
        self.assertEqual(Observation.objects.count(), 1)

        # Set the sensor back to active again
        self.sensor.is_active = True
        self.sensor.save()

    def test_post_new_record(self):
        """ Test posting a new vanilla message """
        post_data = json.loads(TEST_POST)
        response = self.client.post(self.URL, post_data, **AUTHORIZATION_HEADER, format='json')

        # Check the Observation record
        self.assertEqual(response.status_code, 201, response.data)
        self.assertEqual(Observation.objects.all().count(), 1)
        observation = Observation.objects.all()[0]
        fields_to_check = ('sensor', 'sensor_type', 'sensor_state', 'owner', 'supplier', 'purpose', 'latitude',
                           'longitude', 'interval', 'timestamp_message', 'timestamp_start')
        for attr in fields_to_check:
            if type(getattr(observation, attr)) is Decimal:
                self.assertEqual(float(getattr(observation, attr)), post_data['data'][0][attr])
            elif type(getattr(observation, attr)) is datetime:
                self.assertEqual(getattr(observation, attr), parser.parse(post_data['data'][0][attr]))
            else:
                self.assertEqual(getattr(observation, attr), post_data['data'][0][attr])

        # Check the CountAggregate record
        self.assertEqual(CountAggregate.objects.all().count(), len(post_data['data'][0]['aggregate']))
        for count_aggr in CountAggregate.objects.all():


            # Get the post data for this CountAggregate (they might not be in the same order)
            posted_count_aggregate = None
            for aggregate in post_data['data'][0]['aggregate']:
                if aggregate['type'] == count_aggr.type:
                    posted_count_aggregate = aggregate

            if count_aggr.type == 'line':
                fields_to_check = ('type', 'azimuth', 'count_in', 'count_out')
            elif count_aggr.type == 'zone':
                fields_to_check = ('type', 'area', 'geom', 'count')

            # Check whether we actually found the correct posted count aggregate
            self.assertEqual(type(posted_count_aggregate), dict)

            for attr in fields_to_check:
                self.assertEqual(getattr(count_aggr, attr), posted_count_aggregate[attr])
            self.assertEqual(count_aggr.external_id, posted_count_aggregate['id'])
            self.assertEqual(count_aggr.message, post_data['data'][0]['message'])
            self.assertEqual(count_aggr.version, post_data['data'][0]['version'])

        # Check the PersonAggregate
        self.assertEqual(PersonAggregate.objects.all().count(), len(post_data['data'][1]['aggregate']))
        for i, pers_aggr in enumerate(PersonAggregate.objects.all()):
            # Get the post data for this PersonAggregate (they might not be in the same order)
            posted_person_aggregate = None
            for postedPersonAggregate in post_data['data'][1]['aggregate']:
                if postedPersonAggregate['personId'] == str(pers_aggr.person_id):
                    posted_person_aggregate = postedPersonAggregate
                    break

            # Check whether we actually found the correct posted person aggregate
            self.assertEqual(type(posted_person_aggregate), dict)

            # Check the values
            for attr in ('observation_timestamp', 'record', 'speed', 'geom', 'quality', 'distances'):
                if type(getattr(pers_aggr, attr)) is datetime:
                    self.assertEqual(getattr(pers_aggr, attr), parser.parse(posted_person_aggregate[attr]))
                else:
                    self.assertEqual(getattr(pers_aggr, attr), posted_person_aggregate[attr])

            self.assertEqual(str(pers_aggr.person_id), posted_person_aggregate['personId'])

    def test_post_new_record_with_double_zone(self):
        """ Test posting a new message with a double zone in the count message """
        post_data = json.loads(TEST_POST_DOUBLE_ZONE)
        response = self.client.post(self.URL, post_data, **AUTHORIZATION_HEADER, format='json')

        # Check the Observation record
        self.assertEqual(response.status_code, 201, response.data)
        self.assertEqual(Observation.objects.all().count(), 1)
        observation = Observation.objects.get()
        fields_to_check = ('sensor', 'sensor_type', 'sensor_state', 'owner', 'supplier', 'purpose', 'latitude',
                           'longitude', 'interval', 'timestamp_message', 'timestamp_start')
        for attr in fields_to_check:
            if type(getattr(observation, attr)) is Decimal:
                self.assertEqual(float(getattr(observation, attr)), post_data['data'][0][attr])
            elif type(getattr(observation, attr)) is datetime:
                self.assertEqual(getattr(observation, attr), parser.parse(post_data['data'][0][attr]))
            else:
                self.assertEqual(getattr(observation, attr), post_data['data'][0][attr])

        # Check the CountAggregate records
        self.assertEqual(CountAggregate.objects.all().count(), len(post_data['data'][0]['aggregate']))
        for count_aggr in CountAggregate.objects.all():

            # Get the post data for this CountAggregate (they might not be in the same order)
            posted_count_aggregate = None
            for aggregate in post_data['data'][0]['aggregate']:
                if aggregate['id'] == count_aggr.external_id:
                    posted_count_aggregate = aggregate

            # Check whether we actually found the correct posted count aggregate
            self.assertEqual(type(posted_count_aggregate), dict)

            for attr in ('type', 'area', 'count'):
                self.assertEqual(getattr(count_aggr, attr), posted_count_aggregate[attr])
            self.assertEqual(count_aggr.external_id, posted_count_aggregate['id'])
            self.assertEqual(count_aggr.geom, None)  # In both zone counts the geom is an empty string. So we check whether they are None
            self.assertEqual(count_aggr.message, post_data['data'][0]['message'])
            self.assertEqual(count_aggr.version, post_data['data'][0]['version'])

    def test_post_fails_without_token(self):
        response = self.client.post(self.URL, json.loads(TEST_POST), format='json')
        self.assertEqual(response.status_code, 401)
        self.assertEqual(Observation.objects.all().count(), 0)

    def test_sending_the_same_record_twice(self):
        self.client.post(self.URL, json.loads(TEST_POST), **AUTHORIZATION_HEADER, format='json')
        self.client.post(self.URL, json.loads(TEST_POST), **AUTHORIZATION_HEADER, format='json')
        self.assertEqual(Observation.objects.all().count(), 2)
        self.assertEqual(CountAggregate.objects.all().count(), 4)
        self.assertEqual(PersonAggregate.objects.all().count(), 4)

    def test_sending_a_completely_malformed_record(self):
        post_data = json.loads('{"this_is": "malformed data"}')
        response = self.client.post(self.URL, post_data, **AUTHORIZATION_HEADER, format='json')

        self.assertEqual(Observation.objects.all().count(), 0)
        self.assertEqual(CountAggregate.objects.all().count(), 0)
        self.assertEqual(PersonAggregate.objects.all().count(), 0)
        self.assertEqual(response.status_code, 400, response.data)

    def test_geom_fields_to_null(self):
        post_data = json.loads(TEST_POST)
        post_data['data'][0]['aggregate'][1]['geom'] = None
        post_data['data'][1]['aggregate'][0]['geom'] = None
        post_data['data'][1]['aggregate'][1]['geom'] = None
        response = self.client.post(self.URL, post_data, **AUTHORIZATION_HEADER, format='json')
        self.assertEqual(response.status_code, 201)
        self.assertEqual(Observation.objects.all().count(), 1)

    def test_absent_geom_fields(self):
        post_data = json.loads(TEST_POST)
        del post_data['data'][0]['aggregate'][1]['geom']
        del post_data['data'][1]['aggregate'][0]['geom']
        del post_data['data'][1]['aggregate'][1]['geom']
        response = self.client.post(self.URL, post_data, **AUTHORIZATION_HEADER, format='json')
        self.assertEqual(response.status_code, 201)
        self.assertEqual(Observation.objects.all().count(), 1)

    def test_geom_fields_to_empty_string(self):
        post_data = json.loads(TEST_POST)
        post_data['data'][0]['aggregate'][1]['geom'] = ''
        post_data['data'][1]['aggregate'][0]['geom'] = ''
        post_data['data'][1]['aggregate'][1]['geom'] = ''
        response = self.client.post(self.URL, post_data, **AUTHORIZATION_HEADER, format='json')
        self.assertEqual(response.status_code, 201)
        self.assertEqual(Observation.objects.all().count(), 1)

    def test_lat_lng_with_many_decimals(self):
        post_data = json.loads(TEST_POST)
        post_data['data'][0]['latitude'] = 52.3921439524031
        post_data['data'][0]['longitude'] = 4.885872984800177
        post_data['data'][1]['latitude'] = post_data['data'][0]['latitude']
        post_data['data'][1]['longitude'] = post_data['data'][0]['longitude']
        response = self.client.post(self.URL, post_data, **AUTHORIZATION_HEADER, format='json')
        self.assertEqual(response.status_code, 201)
        self.assertEqual(Observation.objects.all().count(), 1)

    def test_405_on_get(self):
        response = self.client.get(self.URL, **AUTHORIZATION_HEADER, format='json')
        self.assertEqual(response.status_code, 405, response.data)

    def test_405_on_put(self):
        response = self.client.put(self.URL, json.loads(TEST_POST), **AUTHORIZATION_HEADER, format='json')
        self.assertEqual(response.status_code, 405, response.data)

    def test_405_on_patch(self):
        response = self.client.patch(self.URL, json.loads(TEST_POST), **AUTHORIZATION_HEADER, format='json')
        self.assertEqual(response.status_code, 405, response.data)

    def test_405_on_delete(self):
        response = self.client.delete(self.URL, json.loads(TEST_POST), **AUTHORIZATION_HEADER, format='json')
        self.assertEqual(response.status_code, 405, response.data)


class ToolsTest(TestCase):
    def test_scramble_counts_vanilla(self):
        count_agg = baker.make(CountAggregate)
        count_agg.count_in = 1
        count_agg.count_out = 1
        count_agg.count = 1
        count_agg.count_in_scrambled = None
        count_agg.count_out_scrambled = None
        count_agg.count_scrambled = None

        count_agg = scramble_count_aggregate(count_agg)

        self.assertIn(count_agg.count_in_scrambled, (0, 1, 2))
        self.assertIn(count_agg.count_out_scrambled, (0, 1, 2))
        self.assertIn(count_agg.count_scrambled, (0, 1, 2))

    def test_scramble_counts_with_counts_none(self):
        count_agg = baker.make(CountAggregate)
        count_agg.count_in = None
        count_agg.count_out = None
        count_agg.count = None
        count_agg.count_in_scrambled = None
        count_agg.count_out_scrambled = None
        count_agg.count_scrambled = None

        count_agg = scramble_count_aggregate(count_agg)

        self.assertIsNone(count_agg.count_in_scrambled)
        self.assertIsNone(count_agg.count_out_scrambled)
        self.assertIsNone(count_agg.count_scrambled)

    def test_scramble_counts_doesnt_overwrite(self):
        count_agg = baker.make(CountAggregate)
        count_agg.count_in = 1
        count_agg.count_out = 1
        count_agg.count = 1
        count_agg.count_in_scrambled = 1
        count_agg.count_out_scrambled = 1
        count_agg.count_scrambled = 1

        count_agg = scramble_count_aggregate(count_agg)

        self.assertEquals(count_agg.count_in_scrambled, 1)
        self.assertEquals(count_agg.count_out_scrambled, 1)
        self.assertEquals(count_agg.count_scrambled, 1)

    def test_scramble_counts_with_counts_zero(self):
        count_agg = baker.make(CountAggregate)
        count_agg.count_in = 0
        count_agg.count_out = 0
        count_agg.count = 0
        count_agg.count_in_scrambled = None
        count_agg.count_out_scrambled = None
        count_agg.count_scrambled = None

        count_agg = scramble_count_aggregate(count_agg)

        self.assertIn(count_agg.count_in_scrambled, (0, 1))
        self.assertIn(count_agg.count_out_scrambled, (0, 1))
        self.assertIn(count_agg.count_scrambled, (0, 1))
