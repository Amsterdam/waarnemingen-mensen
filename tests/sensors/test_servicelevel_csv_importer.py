import os
from unittest import mock

import pytest
from django.contrib.gis.geos import Point
from model_bakery import baker

from peoplemeasurement.models import Servicelevel
from peoplemeasurement.csv_imports.servicelevel_csv_importer import ServicelevelCsvImporter



@pytest.mark.django_db
class TestServicelevelCsvImporter:
    @classmethod
    def setup_class(cls):
        cls.test_files_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "files"
        )

    def test_import_csv_reader(self):
        assert Servicelevel.objects.count() == 0
        test_csv = os.path.join(self.test_files_path, "peoplemeasurement_servicelevel_test.csv")
        ServicelevelCsvImporter(test_csv).import_csv()
        assert Servicelevel.objects.count() == 72

    @mock.patch(
        "peoplemeasurement.csv_imports.servicelevel_csv_importer"
        ".ServicelevelCsvImporter._truncate_servicelevels"
    )
    def test_import_csv_reader_truncate(self, mocked_truncate):
        baker.make(Servicelevel, _quantity=15)
        ServicelevelCsvImporter("")._import_csv_reader([])
        mocked_truncate.assert_called_with()

    @mock.patch(
        "peoplemeasurement.csv_imports.servicelevel_csv_importer"
        ".ServicelevelCsvImporter._create_servicelevel_for_row"
    )
    def test_import_csv_reader_error(self, mocked_create):
        mocked_create.side_effect = Exception

        baker.make(Servicelevel, _quantity=15)
        with pytest.raises(Exception):
            ServicelevelCsvImporter("")._import_csv_reader(csv_reader=[1])
            assert Servicelevel.objects.count() == 15

    def test_create_servicelevel_for_row(self):

        row = dict(
            type_parameter="Count",
            type_gebied="Shopping",
            type_tijd="Peak",
            level_nr="1",
            level_label="Comfortabel",
            lowerlimit="0.5",
            upperlimit="14.5",
        )

        servicelevel = ServicelevelCsvImporter("")._create_servicelevel_for_row(row)
        for key, value in row.items():
            expected_value = value
            if key == "level_nr":
                expected_value = 1
            elif key == "lowerlimit":
                expected_value = 0.5
            elif key == "upperlimit":
                expected_value = 14.5

            value = getattr(servicelevel, key)
            assert (
                value == expected_value
            ), f"Expected Servicelevel.{key} to have value '{expected_value}' but is {value}"

    def test_truncate_servicelevels(self):
        """
        That that we delete all service levels
        """
        baker.make(Servicelevel, _quantity=13)
        ServicelevelCsvImporter("path/to/file.csv")._truncate_servicelevels()
        assert Servicelevel.objects.count() == 0
