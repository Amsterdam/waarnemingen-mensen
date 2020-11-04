from datetime import datetime

from rest_framework.test import APITestCase

from ingress.models import IngressQueue
from tests.tools_for_testing import call_man_command


class IngressTests(APITestCase):
    def setUp(self):
        self.URL = '/ingress/example'
        self.JSON_STR = '{"a": 1, "b": 2}'
        self.XML_STR = '<?xml version="1.0"?><catalog><book id="1"><title>Awesome Book</title></book></catalog>'

    def test_post_json_succeeds(self):
        count_before = IngressQueue.objects.count()
        response = self.client.post(self.URL, self.JSON_STR, content_type='application/json')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(IngressQueue.objects.count(), count_before + 1)
        ingress = IngressQueue.objects.order_by('-id')[0]
        self.assertEqual(ingress.endpoint, 'example')
        self.assertEqual(ingress.raw_data, self.JSON_STR)

    def test_post_xml_succeeds(self):
        count_before = IngressQueue.objects.count()
        response = self.client.post(self.URL, self.XML_STR, content_type='application/xml')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(IngressQueue.objects.count(), count_before + 1)
        ingress = IngressQueue.objects.order_by('-id')[0]
        self.assertEqual(ingress.endpoint, 'example')
        self.assertEqual(ingress.raw_data, self.XML_STR)

    def test_post_wrong_json_succeeds(self):
        count_before = IngressQueue.objects.count()
        response = self.client.post(self.URL, 'NOT CORRECT JSON', content_type='application/json')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(IngressQueue.objects.count(), count_before + 1)
        ingress = IngressQueue.objects.order_by('-id')[0]
        self.assertEqual(ingress.endpoint, 'example')
        self.assertEqual(ingress.raw_data, 'NOT CORRECT JSON')

    def test_post_with_raw_content_type_succeeds(self):
        count_before = IngressQueue.objects.count()
        response = self.client.post(self.URL, 'raw data', content_type='raw')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(IngressQueue.objects.count(), count_before + 1)
        ingress = IngressQueue.objects.order_by('-id')[0]
        self.assertEqual(ingress.endpoint, 'example')
        self.assertEqual(ingress.raw_data, 'raw data')

    def test_post_with_no_content_succeeds(self):
        count_before = IngressQueue.objects.count()
        response = self.client.post(self.URL, '', content_type='raw')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(IngressQueue.objects.count(), count_before + 1)
        ingress = IngressQueue.objects.order_by('-id')[0]
        self.assertEqual(ingress.endpoint, 'example')
        self.assertEqual(ingress.raw_data, '')

    def test_clean_ingress(self):
        count_before = IngressQueue.objects.count()
        self.assertEqual(count_before, 0)

        # Add some records
        for i in range(3):
            self.client.post(self.URL, self.JSON_STR, content_type='application/json')
        self.assertEqual(IngressQueue.objects.count(), 3)

        # Set 2 out of 3 records to succeeded
        for ingress in IngressQueue.objects.order_by('-id')[:2]:
            ingress.parse_succeeded = datetime.utcnow()
            ingress.save()

        # Remove them
        out = call_man_command('clean_ingress')
        self.assertEqual(out.strip(), '2')
        self.assertEqual(IngressQueue.objects.count(), 1)
