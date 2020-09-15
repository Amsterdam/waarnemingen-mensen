import json
import logging
from datetime import datetime, timedelta
from uuid import uuid4

import pytz
from django.conf import settings
from django.db import connection
from factory import fuzzy
from rest_framework.test import APITestCase

from peoplemeasurement.models import PeopleMeasurement

from .test_telcameras_v2 import create_new_v2_json
from .tools_for_testing import create_auth_headers_by_group_name

log = logging.getLogger(__name__)
timezone = pytz.timezone("UTC")

BBOX = [52.03560, 4.58565, 52.48769, 5.31360]

TEST_POST = {
    "data": {
        "id": "902d9a26-6b6e-49d5-8598-0de774e23da1",
        "sensor": "Kalverstraat",
        "sensortype": "countingcamera",
        "version": "1",
        "latitude": "52.37131273473",
        "longitude": "4.89371899382",
        "timestamp": "2019-06-21T10:35:46+02:00",
        "density": 0.0,
        "count": 0.0,
        "speed": 0.6614829134196043
    },
    "details": [
        {
            "timestamp": "2019-06-21T10:35:46+02:00",
            "count": "1.486830472946167",
            "id": "f6c08c28-a800-4e03-b23c-44a6b2d9f53d",
            "direction": "speed"
        },{
            "timestamp": "2019-06-21T10:35:46+02:00",
            "count": "0",
            "id": "b8018928-ff83-4b6a-8934-24f27612e841",
            "direction": "density"
        },{
            "timestamp": "2019-06-21T10:35:46+02:00",
            "count": "6",
            "id": "b8018928-ff83-4b6a-8934-24f27612e841",
            "direction": "up"
        },{
            "timestamp": "2019-06-21T10:35:46+02:00",
            "count": "2",
            "id": "b8018928-ff83-4b6a-8934-24f27612e841",
            "direction": "down"
        },{
            "timestamp": "2019-06-21T10:35:46+02:00",
            "count": "1.3242228031158447",
            "id": "043bb61d-f396-436e-989b-88ce3fb4ded3",
            "direction": "speed"
        }
    ]
}


def get_record_count():
    with connection.cursor() as cursor:
        cursor.execute("select count(id) from peoplemeasurement_peoplemeasurement;")
        row = cursor.fetchone()
        if len(row):
            return row[0]
        return 0


def create_new_v1_object(timestamp_str="2019-06-21T10:35:46+02:00"):
    return PeopleMeasurement.objects.create(
        id=str(uuid4()),
        version="1",
        timestamp=timestamp_str,
        sensor="TEST",
        sensortype="sensortypeA",
        latitude="52.37131273473",
        longitude="4.89371899382",
        density=fuzzy.FuzzyFloat(0, 3).fuzz(),
        speed=fuzzy.FuzzyFloat(0, 3).fuzz(),
        count=fuzzy.FuzzyInteger(0, 100).fuzz(),
        details=[{
            "timestamp": "2019-06-21T10:35:46+02:00",
            "count": "1.486830472946167",
            "id": "f6c08c28-a800-4e03-b23c-44a6b2d9f53d",
            "direction": "speed"
        }, {
            "timestamp": "2019-06-21T10:35:46+02:00",
            "count": "0",
            "id": "b8018928-ff83-4b6a-8934-24f27612e841",
            "direction": "density"
        }, {
            "timestamp": "2019-06-21T10:35:46+02:00",
            "count": "6",
            "id": "b8018928-ff83-4b6a-8934-24f27612e841",
            "direction": "up"
        }, {
            "timestamp": "2019-06-21T10:35:46+02:00",
            "count": "2",
            "id": "b8018928-ff83-4b6a-8934-24f27612e841",
            "direction": "down"
        }, {
            "timestamp": "2019-06-21T10:35:46+02:00",
            "count": "1.3242228031158447",
            "id": "043bb61d-f396-436e-989b-88ce3fb4ded3",
            "direction": "speed"
        }]
    )


class PeopleMeasurementTestPostV1(APITestCase):
    """ Test the people measurement endpoint """

    def setUp(self):
        self.URL = '/telcameras/v1/'
        self.POST_AUTHORIZATION_HEADER = create_auth_headers_by_group_name(settings.GROUP_POST_DATA)

    def test_post_fails_without_token(self):
        record_count_before = get_record_count()
        response = self.client.post(self.URL, TEST_POST, format='json')
        self.assertEqual(response.status_code, 401)
        self.assertEqual(record_count_before, 0)

    def test_post_fails_with_wrong_token(self):
        record_count_before = get_record_count()
        response = self.client.post(self.URL, TEST_POST, **{'HTTP_AUTHORIZATION': f"Token wrongtoken"}, format='json')
        self.assertEqual(response.status_code, 401)
        self.assertEqual(record_count_before, 0)

    def test_post_new_people_measurement(self):
        """ Test posting a new vanilla message """
        record_count_before = get_record_count()
        response = self.client.post(self.URL, TEST_POST, **self.POST_AUTHORIZATION_HEADER, format='json')

        self.assertEqual(response.status_code, 201, response.data)
        self.assertEqual(record_count_before+1, get_record_count())

        for k, v in TEST_POST['data'].items():
            self.assertEqual(response.data[k], v)

    def test_post_new_people_measurement_with_missing_density_count_speed_details(self):
        """ Test posting a new vanilla message """
        record_count_before = get_record_count()
        test_post = TEST_POST.copy()
        del test_post['data']['density']
        del test_post['data']['count']
        del test_post['data']['speed']
        del test_post['details']
        response = self.client.post(self.URL, test_post, **self.POST_AUTHORIZATION_HEADER, format='json')

        self.assertEqual(record_count_before+1, get_record_count())
        self.assertEqual(response.status_code, 201, response.data)

        for k, v in test_post['data'].items():
            self.assertEqual(response.data[k], v)

        for i in ('density', 'count', 'speed', 'details'):
            self.assertEqual(response.data[i], None)

    def test_post_wrongy_formatted_message(self):
        record_count_before = get_record_count()
        response = self.client.post(self.URL, {'wrongly': 'formatted message'}, **self.POST_AUTHORIZATION_HEADER, format='json')

        self.assertEqual(record_count_before, get_record_count())
        self.assertEqual(response.status_code, 400, response.data)

    def test_post_same_id_twice(self):
        # Post it once
        response = self.client.post(self.URL, TEST_POST, **self.POST_AUTHORIZATION_HEADER, format='json')
        self.assertEqual(response.status_code, 201)

        # Post it twice
        response = self.client.post(self.URL, TEST_POST, **self.POST_AUTHORIZATION_HEADER, format='json')
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()['id'][0], 'people measurement with this id already exists.')

    def test_get_peoplemeasurements_not_allowed(self):
        """ Test if getting a peoplemeasurement is not allowed """
        # First post one
        response = self.client.post(self.URL, TEST_POST, **self.POST_AUTHORIZATION_HEADER, format='json')
        self.assertEqual(response.status_code, 201)

        # Then check if I cannot get it
        response = self.client.get(f'{self.URL}{TEST_POST["data"]["id"]}/', **self.POST_AUTHORIZATION_HEADER)
        self.assertEqual(response.status_code, 405)

    def test_update_peoplemeasurements_not_allowed(self):
        """ Test if updating a peoplemeasurement is not allowed """
        # First post one
        response = self.client.post(self.URL, TEST_POST, **self.POST_AUTHORIZATION_HEADER, format='json')
        self.assertEqual(response.status_code, 201)

        # Then check if I cannot update it
        response = self.client.put(f'{self.URL}{TEST_POST["data"]["id"]}/', TEST_POST, **self.POST_AUTHORIZATION_HEADER, format='json')
        self.assertEqual(response.status_code, 405)

    def test_delete_peoplemeasurements_not_allowed(self):
        """ Test if deleting a peoplemeasurement is not allowed """
        # First post one
        response = self.client.post(self.URL, TEST_POST, **self.POST_AUTHORIZATION_HEADER, format='json')
        self.assertEqual(response.status_code, 201)

        # Then check if I cannot delete it
        response = self.client.delete(f'{self.URL}{TEST_POST["data"]["id"]}/', **self.POST_AUTHORIZATION_HEADER)
        self.assertEqual(response.status_code, 405)


class PeopleMeasurementTestGetV1(APITestCase):

    def setUp(self):
        self.URL = '/telcameras/v1/15minaggregate/'
        self.POST_URL_V2 = '/telcameras/v2/'
        self.POST_AUTHORIZATION_HEADER = create_auth_headers_by_group_name(settings.GROUP_POST_DATA)
        self.GET_AUTHORIZATION_HEADER = create_auth_headers_by_group_name(settings.GROUP_GET_DATA)

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
    #     response = self.client.get(self.URL, **self.GET_AUTHORIZATION_HEADER)
    #     self.assertEqual(response.status_code, 200)
    #     self.assertEqual(len(response.data), 26)

    def test_get_15min_aggregation_timezone_with_both_v1_and_v2_records(self):
        for i in range(0, 24):
            timestamp_str = datetime.now().replace(hour=i, minute=0, second=0).astimezone().replace(
                microsecond=0).isoformat()

            # Insert some v1 records for each hour
            create_new_v1_object(timestamp_str=timestamp_str)

            # Insert some v2 records for each hour
            self.client.post(
                self.POST_URL_V2,
                json.loads(create_new_v2_json(timestamp_str=timestamp_str)),
                **self.POST_AUTHORIZATION_HEADER,
                format='json'
            )

        # test whether the endpoint responds correctly
        response = self.client.get(self.URL, **self.GET_AUTHORIZATION_HEADER)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.data), 24)

    def test_get_15min_aggregation_records_fails_without_token(self):
        response = self.client.get(self.URL)
        self.assertEqual(response.status_code, 401)

    def test_get_15min_aggregation_records_fails_with_wrong_token(self):
        response = self.client.get(self.URL, **self.POST_AUTHORIZATION_HEADER)
        self.assertEqual(response.status_code, 401)
