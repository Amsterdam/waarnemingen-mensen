import json
import logging
from uuid import uuid4

import pytz
from django.db import connection
from rest_framework.test import APITestCase

from factory import fuzzy
from .models import PeopleMeasurement

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
            "id": "d6129055-6427-44dc-923b-dd903c2e7f98",
            "direction": "up"
        },{
            "timestamp": "2019-06-21T10:35:46+02:00",
            "count": "2",
            "id": "bba2f11f-b07b-4f27-b66d-2963ae3029bb",
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


def create_new_object(store):
    peoplemeasurement = PeopleMeasurement(
        id=str(uuid4()),
        version="1",
        timestamp="2019-06-21T10:35:46+02:00",
        sensor="sensorX",
        sensortype="sensortypeA",
        latitude="52.37131273473",
        longitude="4.89371899382",
        density=fuzzy.FuzzyFloat(0, 3).fuzz(),
        speed=fuzzy.FuzzyFloat(0, 3).fuzz(),
        count=fuzzy.FuzzyInteger(0, 100).fuzz(),
        details=json.dumps([{
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
        }
        ])
    )

    if store:
        peoplemeasurement.save()
    return peoplemeasurement


class PeopleMeasurementTestV1(APITestCase):
    """ Test the people measurement endpoint """

    def setUp(self):
        self.URL = '/telcameras/v1/'

    def test_post_new_people_measurement(self):
        """ Test posting a new vanilla message """
        record_count_before = get_record_count()
        response = self.client.post(self.URL, TEST_POST, format='json')
    
        self.assertEqual(record_count_before+1, get_record_count())
        self.assertEqual(response.status_code, 201, response.data)
    
        for k, v in TEST_POST['data'].items():
            self.assertEqual(response.data[k], v)
    
        newest_record = PeopleMeasurement.objects.order_by('timestamp').last()
        self.assertEqual(newest_record.measurementdetail_set.all().count(), len(TEST_POST['details']))
    
        for detail in newest_record.measurementdetail_set.all():
            posted_details = [d for d in TEST_POST['details'] if d['id'] == str(detail.id)]
            self.assertEqual(len(posted_details), 1)
            self.assertEqual(detail.direction, posted_details[0]['direction'])
            self.assertAlmostEqual(detail.count, float(posted_details[0]['count']))

    def test_post_new_people_measurement_with_missing_density_count_speed_details(self):
        """ Test posting a new vanilla message """
        record_count_before = get_record_count()
        test_post = TEST_POST.copy()
        del test_post['data']['density']
        del test_post['data']['count']
        del test_post['data']['speed']
        del test_post['details']
        response = self.client.post(self.URL, test_post, format='json')

        self.assertEqual(record_count_before+1, get_record_count())
        self.assertEqual(response.status_code, 201, response.data)

        for k, v in test_post['data'].items():
            self.assertEqual(response.data[k], v)

        for i in ('density', 'count', 'speed', 'details'):
            self.assertEqual(response.data[i], None)

        newest_record = PeopleMeasurement.objects.order_by('timestamp').last()
        self.assertEqual(newest_record.measurementdetail_set.all().count(), 0)

    def test_get_peoplemeasurements_not_allowed(self):
        """ Test if getting a peoplemeasurement is not allowed """
        # First post one
        response = self.client.post(self.URL, TEST_POST, format='json')
        self.assertEqual(response.status_code, 201)
    
        # Then check if I cannot get it
        response = self.client.get(f'{self.URL}{TEST_POST["data"]["id"]}/')
        self.assertEqual(response.status_code, 405)
    
    def test_update_peoplemeasurements_not_allowed(self):
        """ Test if updating a peoplemeasurement is not allowed """
        # First post one
        response = self.client.post(self.URL, TEST_POST, format='json')
        self.assertEqual(response.status_code, 201)
    
        # Then check if I cannot update it
        response = self.client.put(f'{self.URL}{TEST_POST["data"]["id"]}/', TEST_POST, format='json')
        self.assertEqual(response.status_code, 405)
    
    def test_delete_peoplemeasurements_not_allowed(self):
        """ Test if deleting a peoplemeasurement is not allowed """
        # First post one
        response = self.client.post(self.URL, TEST_POST, format='json')
        self.assertEqual(response.status_code, 201)
    
        response = self.client.delete(f'{self.URL}{TEST_POST["data"]["id"]}/')
        self.assertEqual(response.status_code, 405)
