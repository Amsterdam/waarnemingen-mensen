from unittest import TestCase, mock

import pytest
from django.test import Client


@pytest.mark.django_db
class TestViews(TestCase):
    def setUp(self):
        self.http_client = Client()

    def test_health_view(self):
        response = self.http_client.get("/status/health")
        assert response.status_code == 200
        assert response.content == b"Connectivity OK"

    @mock.patch("health.views.settings.DEBUG", True)
    def test_debug_false(self):
        response = self.http_client.get("/status/health")
        assert response.status_code == 500
        assert response.content == b"Debug mode not allowed in production"

    @mock.patch("django.db.connection.cursor")
    def test_database_error(self, mocked_cursor):
        mocked_cursor.side_effect = RuntimeError("database error")
        response = self.http_client.get("/status/health")
        assert response.status_code == 500
        assert response.content == b"Database connectivity failed"
