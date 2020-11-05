from datetime import datetime

from rest_framework.test import APITestCase

from ingress.models import IngressQueue, Endpoint
from tests.tools_for_testing import call_man_command


class IngressTests(APITestCase):
    def setUp(self):
        self.endpoint_url_key = 'example'
        self.URL = '/ingress/' + self.endpoint_url_key
        self.JSON_STR = '{"a": 1, "b": 2}'
        self.XML_STR = '<?xml version="1.0"?><catalog><book id="1"><title>Awesome Book</title></book></catalog>'

        # Create an endpoint
        self.endpoint_obj = Endpoint.objects.create(url_key = self.endpoint_url_key)

    def test_post_json_succeeds(self):
        count_before = IngressQueue.objects.count()
        response = self.client.post(self.URL, self.JSON_STR, content_type='application/json')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(IngressQueue.objects.count(), count_before + 1)
        ingress = IngressQueue.objects.order_by('-id')[0]
        self.assertEqual(ingress.endpoint.url_key, 'example')
        self.assertEqual(ingress.raw_data, self.JSON_STR)

    def test_post_xml_succeeds(self):
        count_before = IngressQueue.objects.count()
        response = self.client.post(self.URL, self.XML_STR, content_type='application/xml')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(IngressQueue.objects.count(), count_before + 1)
        ingress = IngressQueue.objects.order_by('-id')[0]
        self.assertEqual(ingress.endpoint.url_key, 'example')
        self.assertEqual(ingress.raw_data, self.XML_STR)

    def test_post_wrong_json_succeeds(self):
        count_before = IngressQueue.objects.count()
        response = self.client.post(self.URL, 'NOT CORRECT JSON', content_type='application/json')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(IngressQueue.objects.count(), count_before + 1)
        ingress = IngressQueue.objects.order_by('-id')[0]
        self.assertEqual(ingress.endpoint.url_key, 'example')
        self.assertEqual(ingress.raw_data, 'NOT CORRECT JSON')

    def test_post_with_raw_content_type_succeeds(self):
        count_before = IngressQueue.objects.count()
        response = self.client.post(self.URL, 'raw data', content_type='raw')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(IngressQueue.objects.count(), count_before + 1)
        ingress = IngressQueue.objects.order_by('-id')[0]
        self.assertEqual(ingress.endpoint.url_key, 'example')
        self.assertEqual(ingress.raw_data, 'raw data')

    def test_post_with_no_content_succeeds(self):
        count_before = IngressQueue.objects.count()
        response = self.client.post(self.URL, '', content_type='raw')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(IngressQueue.objects.count(), count_before + 1)
        ingress = IngressQueue.objects.order_by('-id')[0]
        self.assertEqual(ingress.endpoint.url_key, 'example')
        self.assertEqual(ingress.raw_data, '')

    def test_post_with_no_endpoint_url_key_fails(self):
        response = self.client.post('/ingress/', 'data', content_type='raw')
        self.assertEqual(response.status_code, 404)

    def test_post_to_non_existing_endpoint_fails(self):
        response = self.client.post('/ingress/doesnotexist', 'data', content_type='raw')
        self.assertEqual(response.status_code, 404)

    def test_add_endpoint_command(self):
        url_key = 'new_nice-endpoint'  # Make sure it allows for underscores and dashes
        count_before = Endpoint.objects.count()

        # Add the endpoint
        out = call_man_command('add_endpoint', url_key)

        self.assertEqual(out.strip(), f"Created endpoint with url_key '{url_key}'")
        self.assertEqual(Endpoint.objects.count(), count_before + 1)
        endpoint = Endpoint.objects.filter(url_key=url_key).get()
        self.assertEqual(endpoint.url_key, url_key)

    def test_add_endpoint_command_fails_with_too_long_url_key(self):
        url_key = 'a' * 260
        count_before = Endpoint.objects.count()

        out = call_man_command('add_endpoint', url_key)
        self.assertEqual(out.strip(), "The url_key is larger than 255 characters. Please choose a shorter url_key.")
        self.assertEqual(Endpoint.objects.count(), count_before)

    def test_add_endpoint_command_fails_with_weird_characters(self):
        url_key = '!'  # Anything other than numbers, letters, underscores or dashes are not allowed
        count_before = Endpoint.objects.count()

        out = call_man_command('add_endpoint', url_key)
        self.assertEqual(out.strip(), "The url_key can only contain numbers, letters, underscores and dashes.")
        self.assertEqual(Endpoint.objects.count(), count_before)

    def test_add_endpoint_command_fails_with_existing_endpoint(self):
        url_key = 'noice_endpoint'
        count_before = Endpoint.objects.count()

        # Add the endpoint once
        call_man_command('add_endpoint', url_key)
        self.assertEqual(Endpoint.objects.count(), count_before + 1)

        # Try to add the same endpoint again
        out = call_man_command('add_endpoint', url_key)
        self.assertEqual(out.strip(), f"The endpoint '{url_key}' already exists")
        self.assertEqual(Endpoint.objects.count(), count_before + 1)

    def test_remove_endpoint_command(self):
        url_key = 'this_endpoint_should_be_removed'
        count_before = Endpoint.objects.count()

        # First add the endpoint
        call_man_command('add_endpoint', url_key)
        self.assertEqual(Endpoint.objects.count(), count_before + 1)

        # Then remove the endpoint
        out = call_man_command('remove_endpoint', url_key)
        self.assertEqual(Endpoint.objects.count(), count_before)
        self.assertEqual(out.strip(), f"Successfully removed the endpoint '{url_key}'")

    def test_remove_endpoint_command_fails_if_endpoint_does_not_exist(self):
        url_key = 'the_endpoint_to_remove'
        count_before = Endpoint.objects.count()

        # Try to remove the endpoint
        out = call_man_command('remove_endpoint', url_key)
        self.assertEqual(Endpoint.objects.count(), count_before)
        self.assertEqual(out.strip(), f"The endpoint '{url_key}' doesn't exist yet. Nothing has been done.")

    def test_clean_ingress_command(self):
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
