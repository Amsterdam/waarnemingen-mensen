from unittest.mock import patch

from django.test import TestCase
from django.core.management import call_command
from peoplemeasurement.models import PeopleMeasurementCSV, PeopleMeasurementCSVTemp


class TestImportPeopleMeasurementCSV(TestCase):
    """Test writing to database."""

    @patch("peoplemeasurement.objectstore_util.get_objstore_directory_meta")
    @patch("peoplemeasurement.objectstore_util.get_objstore_file")
    def test_import_csv(self, file_stream, meta_list):
        csvfile = open('peoplemeasurement/tests/csv_mock.csv', 'rb').read()

        meta_list.return_value = iter([{'name': 'csv_mock.csv'}])
        file_stream.side_effect = [['', csvfile]]

        call_command('import_peoplemeasurement_csv')

        clean_count = PeopleMeasurementCSV.objects.count()
        self.assertEqual(clean_count, 44)
        self.assertEqual(
            PeopleMeasurementCSV.objects.values('csv_name').distinct()[0],
            'csv_mock.csv'
        )
