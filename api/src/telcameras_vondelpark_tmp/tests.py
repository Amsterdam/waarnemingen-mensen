import logging

import pytz
from rest_framework.test import APITestCase

from .models import JsonDump

log = logging.getLogger(__name__)
timezone = pytz.timezone("UTC")

TEST_POST = {
    'some': 1243,
    'random': 'fields'
}


class PeopleMeasurementTestV1(APITestCase):
    """ Test the people measurement endpoint """

    def setUp(self):
        self.URL = '/telcameras/vondelpark_tmp/'

    def test_post_new_json(self):
        """ Test posting a new json message """
        response = self.client.post(self.URL, TEST_POST, format='json')

        self.assertEqual(response.status_code, 201, response.data)
        self.assertEqual(JsonDump.objects.count(), 1)
        jd = JsonDump.objects.first()
        self.assertEqual(jd.dump, TEST_POST)
