import logging
from factory import fuzzy
import pytz
import json
from rest_framework.test import APITestCase
from django.db import connection
from peoplemeasurement.models import PeopleMeasurement
from uuid import uuid4

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


class PeopleMeasurementTestV0(APITestCase):
    """ Test the people measurement endpoint """

    def setUp(self):
        self.URL = '/v0/people/measurement/'

    def test_post_new_people_measurement(self):
        """ Test posting a new vanilla message """
        record_count_before = get_record_count()
        response = self.client.post(self.URL, TEST_POST, format='json')

        self.assertEqual(record_count_before+1, get_record_count())
        self.assertEqual(response.status_code, 201, response.data)

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
        response = self.client.post(self.URL, test_post, format='json')

        self.assertEqual(record_count_before+1, get_record_count())
        self.assertEqual(response.status_code, 201, response.data)

        for k, v in test_post['data'].items():
            self.assertEqual(response.data[k], v)

        for i in ('density', 'count', 'speed', 'details'):
            self.assertEqual(response.data[i], None)

    def test_list_peoplemeasurements(self):
        """ Test listing all peoplemeasurements """

        ## First store some records
        pms = [
            create_new_object(store=True),
            create_new_object(store=True),
            create_new_object(store=True)
        ]

        # Do the call
        get_response = self.client.get(self.URL)
        self.assertEqual(get_response.status_code, 200)
        self.assertEqual(len(get_response.data['results']), len(pms))

        # Test contents
        for i, item in enumerate(get_response.data['results']):
            for k, _ in TEST_POST['data'].items():
                self.assertEqual(item[k], getattr(pms[i], k))

    def test_get_peoplemeasurement(self):
        """ Test getting a peoplemeasurement """
        record = create_new_object(store=True)
        res = self.client.get(f'{self.URL}{record.id}/')

        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.data['id'], record.id)

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

    def test_default_response_is_json(self):
        """ Test if the default response of the API is on JSON """
        response = self.client.get(self.URL)
        self.assertEqual(200, response.status_code, f"Wrong response code for {self.URL}")
        self.assertEqual('application/json', response["Content-Type"], f"Wrong Content-Type for {self.URL}")

    def test_version_filters(self):
        """ Test filtering on date"""
        # Create some records with different version values
        o1 = create_new_object(store=True)
        o2 = create_new_object(store=False)
        o2.version = "2"
        o2.save()

        # Do a request with a version filter and check if the result is correct
        url = f'{self.URL}?version=1'
        response = self.client.get(url)
        self.assertEqual(len(response.data['results']), 1)
        self.assertEqual(response.data['results'][0]['id'], o1.id)
