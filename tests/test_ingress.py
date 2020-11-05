from collections import namedtuple
from datetime import datetime

from rest_framework.test import APITestCase

from ingress.models import Endpoint, FailedIngressQueue, IngressQueue
from ingress.parser import IngressParser
from tests.tools_for_testing import call_man_command


class TestIngressEndpointManipulation(APITestCase):
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


class TestIngressQueue(APITestCase):
    def setUp(self):
        self.endpoint_url_key = 'example'
        self.URL = '/ingress/' + self.endpoint_url_key
        self.JSON_STR = '{"a": 1, "b": 2}'
        self.XML_STR = '<?xml version="1.0"?><catalog><book id="1"><title>Awesome Book</title></book></catalog>'

        # Create an endpoint
        call_man_command('add_endpoint', self.endpoint_url_key)

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


class MockParser(IngressParser):
    endpoint_url_key = 'parsing_example'
    returned_id = None

    def parse_single_message(self, ingress_raw_data):
        MockedModel = namedtuple('MockModelInstance', 'id')
        obj = MockedModel(id=self.returned_id)
        return obj


class MockParserWithException(IngressParser):
    endpoint_url_key = 'parsing_example'

    def parse_single_message(self, ingress_raw_data):
        1/0


class TestIngressParsing(APITestCase):
    def setUp(self):
        # Create an endpoint
        self.endpoint_url_key = 'parsing_example'
        self.URL = '/ingress/' + self.endpoint_url_key
        self.endpoint_obj = Endpoint.objects.create(url_key=self.endpoint_url_key)

    def test_parsing_succeeded(self):
        count_before = IngressQueue.objects.count()
        self.assertEqual(count_before, 0)

        # Add some records
        for i in range(3):
            self.client.post(self.URL, "the data", content_type='raw')
        self.assertEqual(IngressQueue.objects.count(), 3)

        # Parse records
        parser = MockParser()
        parser.returned_id = 1  # This means the parsing succeeded
        parser.parse_n()

        # Check whether they all have been marked as successful
        for ingress in IngressQueue.objects.filter(endpoint=self.endpoint_obj):
            self.assertIsNotNone(ingress.parse_succeeded)

        # Clean the ingress
        call_man_command('clean_ingress')

        # Check whether they left the queue
        self.assertEqual(IngressQueue.objects.count(), 0)

    def test_parsing_failed(self):
        self.assertEqual(IngressQueue.objects.count(), 0)
        self.assertEqual(FailedIngressQueue.objects.count(), 0)

        # Add some records
        for i in range(3):
            self.client.post(self.URL, "the data", content_type='raw')
        self.assertEqual(IngressQueue.objects.count(), 3)

        # Parse records
        parser = MockParser()
        parser.returned_id = None  # This means the parsing failed
        parser.parse_n()

        # Check whether they left the queue
        self.assertEqual(IngressQueue.objects.count(), 0)

        # Check whether they were added to the failed queue
        self.assertEqual(FailedIngressQueue.objects.count(), 3)
        for failed_ingress in FailedIngressQueue.objects.filter(endpoint=self.endpoint_obj):
            self.assertIsNone(failed_ingress.parse_succeeded)
            self.assertIsNotNone(failed_ingress.parse_failed)
            self.assertIsNone(failed_ingress.parse_fail_info)

    def test_parsing_fails_by_exception(self):
        self.assertEqual(IngressQueue.objects.count(), 0)
        self.assertEqual(FailedIngressQueue.objects.count(), 0)

        # Add some records
        for i in range(3):
            self.client.post(self.URL, "the data", content_type='raw')
        self.assertEqual(IngressQueue.objects.count(), 3)

        # Parse records
        parser = MockParserWithException()
        parser.parse_n()

        # Check whether they left the queue
        self.assertEqual(IngressQueue.objects.count(), 0)

        # Check whether they were added to the failed queue
        self.assertEqual(FailedIngressQueue.objects.count(), 3)
        for failed_ingress in FailedIngressQueue.objects.filter(endpoint=self.endpoint_obj):
            self.assertIsNone(failed_ingress.parse_succeeded)
            self.assertIsNotNone(failed_ingress.parse_failed)
            self.assertContains(failed_ingress.parse_fail_info, 'ZeroDivisionError: division by zero')

