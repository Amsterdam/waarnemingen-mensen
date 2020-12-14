from collections import namedtuple
from datetime import datetime

from django.conf import settings
from rest_framework.test import APITestCase, APITransactionTestCase

from ingress.models import Endpoint, FailedIngressQueue, IngressQueue
from ingress.parser import IngressParser
from tests.tools_for_testing import call_man_command

AUTHORIZATION_HEADER = {'HTTP_AUTHORIZATION': f"Token {settings.AUTHORIZATION_TOKEN}"}


class TestIngressEndpointCommands(APITransactionTestCase):
    reset_sequences = True

    def test_add_endpoint_command(self):
        url_key = 'new_nice-endpoint'  # Make sure it allows for underscores and dashes
        count_before = Endpoint.objects.count()

        # Add the endpoint
        out = call_man_command('add_endpoint', url_key)

        self.assertEqual(out.strip(), f"Created endpoint with url_key '{url_key}'")
        self.assertEqual(Endpoint.objects.count(), count_before + 1)
        endpoint = Endpoint.objects.filter(url_key=url_key).get()
        self.assertEqual(endpoint.url_key, url_key)
        self.assertEqual(endpoint.is_active, True)
        self.assertEqual(endpoint.parser_enabled, False)

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

    def test_list_endpoints_with_no_existing_endpoints(self):
        self.assertEqual(Endpoint.objects.count(), 0)
        out = call_man_command('list_endpoints')
        self.assertEqual(out, "\nCurrent number of endpoints: 0\n\n")

    def test_list_endpoints_with_two_existing_endpoints(self):
        # First add two endpoints
        call_man_command('add_endpoint', 'first_endpoint')
        call_man_command('add_endpoint', 'second_endpoint')
        self.assertEqual(Endpoint.objects.count(), 2)

        # Add an unparsed message for the first endpoint
        first_endpoint = Endpoint.objects.get(url_key='first_endpoint')
        IngressQueue.objects.create(endpoint=first_endpoint, raw_data='the data')

        # Add a failed message for the second endpoint
        second_endpoint = Endpoint.objects.get(url_key='second_endpoint')
        FailedIngressQueue.objects.create(endpoint=second_endpoint, raw_data='the data')

        out = call_man_command('list_endpoints')
        expected_output = '\nCurrent number of endpoints: 2\n\n' \
                          'id   url_key                   is_active  parser_enabled  unparsed   failed     full url            \n' \
                          '1    first_endpoint            1          0               1          0          /ingress/first_endpoint\n' \
                          '2    second_endpoint           1          0               0          1          /ingress/second_endpoint\n'
        self.assertEqual(out, expected_output)

    def test_enable_and_disable_parser(self):
        # First add two endpoints
        call_man_command('add_endpoint', 'first_endpoint')
        call_man_command('add_endpoint', 'second_endpoint')
        self.assertEqual(Endpoint.objects.count(), 2)

        # Try to disable the parser of one of the endpoints and make sure it fails
        out = call_man_command('disable_parser', 'first_endpoint')
        expected_output = "The parser for the endpoint 'first_endpoint' was already disabled. No changes were made.\n"
        self.assertEqual(out, expected_output)
        endpoint = Endpoint.objects.get(url_key='first_endpoint')
        self.assertFalse(endpoint.parser_enabled)

        # Enable the parser of one of the endpoints
        out = call_man_command('enable_parser', 'first_endpoint')
        expected_output = "Enabled the parser for the endpoint with url_key 'first_endpoint'\n"
        self.assertEqual(out, expected_output)
        endpoint = Endpoint.objects.get(url_key='first_endpoint')
        self.assertTrue(endpoint.parser_enabled)

        # Try to enable the parser of the previously enabled endpoint again and make sure it fails
        out = call_man_command('enable_parser', 'first_endpoint')
        expected_output = "The parser for the endpoint 'first_endpoint' was already enabled. No changes were made.\n"
        self.assertEqual(out, expected_output)
        endpoint = Endpoint.objects.get(url_key='first_endpoint')
        self.assertTrue(endpoint.parser_enabled)

        # disable the parser of one of the endpoints
        out = call_man_command('disable_parser', 'first_endpoint')
        expected_output = "Disabled the parser for the endpoint with url_key 'first_endpoint'\n"
        self.assertEqual(out, expected_output)
        endpoint = Endpoint.objects.get(url_key='first_endpoint')
        self.assertFalse(endpoint.parser_enabled)

    def test_redo_failed_messages_moves_messages_from_failed_queue_back_to_normal_queue(self):
        # First add an endpoint
        call_man_command('add_endpoint', 'first_endpoint')
        call_man_command('add_endpoint', 'second_endpoint')
        self.assertEqual(Endpoint.objects.count(), 2)

        # Add a couple failed messages for both the endpoints
        first_endpoint = Endpoint.objects.get(url_key='first_endpoint')
        second_endpoint = Endpoint.objects.get(url_key='second_endpoint')
        for _ in range(3):
            FailedIngressQueue.objects.create(endpoint=first_endpoint, raw_data='the data')
            FailedIngressQueue.objects.create(endpoint=second_endpoint, raw_data='the data')
        self.assertEqual(IngressQueue.objects.count(), 0)
        self.assertEqual(FailedIngressQueue.objects.count(), 6)

        # Move messages for endpoint one from the failed to the normal queue
        out = call_man_command('redo_failed_ingress_messages', 'first_endpoint')
        expected_output = '\n\nMoved 3 messages from the failed queue to the normal queue to be parsed again.\n\n'
        self.assertEqual(out, expected_output)
        self.assertEqual(IngressQueue.objects.count(), 3)
        self.assertEqual(FailedIngressQueue.objects.count(), 3)

    def test_redo_failed_messages_fails_with_non_existing_endpoint(self):
        out = call_man_command('redo_failed_ingress_messages', 'non_existing_endpoint')
        expected_output = "\n\nThe endpoint with url_key 'non_existing_endpoint' does not exist. Nothing has been done.\n\n"
        self.assertEqual(out, expected_output)


class TestIngressQueue(APITestCase):
    def setUp(self):
        self.endpoint_url_key = 'example'
        self.URL = '/ingress/' + self.endpoint_url_key
        self.JSON_STR = '{"a": 1, "b": 2}'
        self.XML_STR = '<?xml version="1.0"?><catalog><book id="1"><title>Awesome Book</title></book></catalog>'

        # Create an endpoint
        call_man_command('add_endpoint', self.endpoint_url_key)

    def test_post_fails_without_token(self):
        count_before = IngressQueue.objects.count()
        response = self.client.post(self.URL, self.JSON_STR, content_type='application/json')
        self.assertEqual(response.status_code, 401)
        self.assertEqual(IngressQueue.objects.count(), count_before)

    def test_post_fails_with_wrong_token(self):
        count_before = IngressQueue.objects.count()
        response = self.client.post(self.URL, self.JSON_STR, HTTP_AUTHORIZATION=f"Token wrong", content_type='application/json')
        self.assertEqual(response.status_code, 403)
        self.assertEqual(IngressQueue.objects.count(), count_before)

    def test_post_json_succeeds(self):
        count_before = IngressQueue.objects.count()
        response = self.client.post(self.URL, self.JSON_STR, **AUTHORIZATION_HEADER, content_type='application/json')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(IngressQueue.objects.count(), count_before + 1)
        ingress = IngressQueue.objects.order_by('-id')[0]
        self.assertEqual(ingress.endpoint.url_key, 'example')
        self.assertEqual(ingress.raw_data, self.JSON_STR)

    def test_post_xml_succeeds(self):
        count_before = IngressQueue.objects.count()
        response = self.client.post(self.URL, self.XML_STR, **AUTHORIZATION_HEADER, content_type='application/xml')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(IngressQueue.objects.count(), count_before + 1)
        ingress = IngressQueue.objects.order_by('-id')[0]
        self.assertEqual(ingress.endpoint.url_key, 'example')
        self.assertEqual(ingress.raw_data, self.XML_STR)

    def test_post_invalid_json_succeeds(self):
        count_before = IngressQueue.objects.count()
        response = self.client.post(self.URL, 'INVALID JSON', **AUTHORIZATION_HEADER, content_type='application/json')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(IngressQueue.objects.count(), count_before + 1)
        ingress = IngressQueue.objects.order_by('-id')[0]
        self.assertEqual(ingress.endpoint.url_key, 'example')
        self.assertEqual(ingress.raw_data, 'INVALID JSON')

    def test_post_with_raw_content_type_succeeds(self):
        count_before = IngressQueue.objects.count()
        response = self.client.post(self.URL, 'raw data', **AUTHORIZATION_HEADER, content_type='raw')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(IngressQueue.objects.count(), count_before + 1)
        ingress = IngressQueue.objects.order_by('-id')[0]
        self.assertEqual(ingress.endpoint.url_key, 'example')
        self.assertEqual(ingress.raw_data, 'raw data')

    def test_post_with_no_content_succeeds(self):
        count_before = IngressQueue.objects.count()
        response = self.client.post(self.URL, '', **AUTHORIZATION_HEADER, content_type='raw')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(IngressQueue.objects.count(), count_before + 1)
        ingress = IngressQueue.objects.order_by('-id')[0]
        self.assertEqual(ingress.endpoint.url_key, 'example')
        self.assertEqual(ingress.raw_data, '')

    def test_post_with_no_endpoint_url_key_fails(self):
        response = self.client.post('/ingress/', 'data', **AUTHORIZATION_HEADER, content_type='raw')
        self.assertEqual(response.status_code, 404)

    def test_post_to_non_existing_endpoint_fails(self):
        response = self.client.post('/ingress/doesnotexist', 'data', **AUTHORIZATION_HEADER, content_type='raw')
        self.assertEqual(response.status_code, 404)

    def test_inactive_endpoint_serves_404(self):
        # Set the endpoint to inactive
        endpoint = Endpoint.objects.get(url_key=self.endpoint_url_key)
        endpoint.is_active = False
        endpoint.save()

        # Call the inactive endpoint
        response = self.client.post(self.URL, 'data', **AUTHORIZATION_HEADER, content_type='raw')
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.content.decode("utf-8"), "Endpoint is not active anymore")

        # Set the endpoint back to active
        endpoint.is_active = True
        endpoint.save()

    def test_clean_ingress_command(self):
        count_before = IngressQueue.objects.count()
        self.assertEqual(count_before, 0)

        # Add some records
        for i in range(3):
            self.client.post(self.URL, self.JSON_STR, **AUTHORIZATION_HEADER, content_type='application/json')
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

    def parse_single_message(self, ingress_raw_data):
        MockedModel = namedtuple('MockModel', 'id')
        obj = MockedModel(id=1)
        return obj


class MockParserWithException(IngressParser):
    endpoint_url_key = 'parsing_example'

    def parse_single_message(self, ingress_raw_data):
        1/0


class TestIngressParsing(APITransactionTestCase):
    def setUp(self):
        # Create an endpoint
        self.endpoint_url_key = 'parsing_example'
        self.URL = '/ingress/' + self.endpoint_url_key
        self.endpoint_obj = Endpoint.objects.create(url_key=self.endpoint_url_key, parser_enabled=True)

    def test_parsing_succeeded(self):
        count_before = IngressQueue.objects.count()
        self.assertEqual(count_before, 0)

        # Add some records
        for i in range(3):
            self.client.post(self.URL, "the data", **AUTHORIZATION_HEADER, content_type='raw')
        self.assertEqual(IngressQueue.objects.count(), 3)

        # Parse records
        parser = MockParser()
        parser.parse_continuously(end_at_empty_queue=True)

        # Check whether they all have been marked as successful
        for ingress in IngressQueue.objects.filter(endpoint=self.endpoint_obj):
            self.assertIsNotNone(ingress.parse_succeeded)

        # Clean the ingress
        call_man_command('clean_ingress')

        # Check whether they left the queue
        self.assertEqual(IngressQueue.objects.count(), 0)

    def test_parsing_fails_by_exception(self):
        self.assertEqual(IngressQueue.objects.count(), 0)
        self.assertEqual(FailedIngressQueue.objects.count(), 0)

        # Add some records
        for i in range(3):
            self.client.post(self.URL, "the data", **AUTHORIZATION_HEADER, content_type='raw')
        self.assertEqual(IngressQueue.objects.count(), 3)

        # Parse records
        parser = MockParserWithException()
        parser.parse_continuously(end_at_empty_queue=True)

        # Check whether they left the queue
        self.assertEqual(IngressQueue.objects.count(), 0)

        # Check whether they were added to the failed queue
        self.assertEqual(FailedIngressQueue.objects.count(), 3)
        for failed_ingress in FailedIngressQueue.objects.filter(endpoint=self.endpoint_obj):
            self.assertIsNone(failed_ingress.parse_succeeded)
            self.assertIsNotNone(failed_ingress.parse_failed)
            self.assertIn('ZeroDivisionError: division by zero', failed_ingress.parse_fail_info)

    def test_parsing_does_not_work_when_parsing_is_disabled(self):
        count_before = IngressQueue.objects.count()
        self.assertEqual(count_before, 0)

        # Add some records
        for i in range(3):
            self.client.post(self.URL, "the data", **AUTHORIZATION_HEADER, content_type='raw')
        self.assertEqual(IngressQueue.objects.count(), 3)

        # Disable the endpoint
        self.endpoint_obj.parser_enabled = False
        self.endpoint_obj.save()

        # Parse records
        parser = MockParser()
        parser.parse_continuously(end_at_empty_queue=True, end_at_disabled_parser=True)

        # Check whether they have not been parsed like expected
        for ingress in IngressQueue.objects.filter(endpoint=self.endpoint_obj):
            self.assertIsNone(ingress.parse_started)
