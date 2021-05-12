import os
from unittest import mock

import pytest
from model_bakery import baker

from peoplemeasurement.models import VoorspelIntercept
from peoplemeasurement.csv_imports.voorspelintercept_csv_importer import VoorspelInterceptCsvImporter


@pytest.mark.django_db
class TestVoorspelInterceptCsvImporter:
    @classmethod
    def setup_class(cls):
        cls.test_files_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "files"
        )

    def test_import_csv_reader(self):
        assert VoorspelIntercept.objects.count() == 0
        test_csv = os.path.join(self.test_files_path, "peoplemeasurement_voorspelintercepts_test.csv")
        VoorspelInterceptCsvImporter(test_csv).import_csv()
        assert VoorspelIntercept.objects.count() == 56

    @mock.patch(
        "peoplemeasurement.csv_imports.voorspelintercept_csv_importer"
        ".VoorspelInterceptCsvImporter._truncate"
    )
    def test_import_csv_reader_truncate(self, mocked_truncate):
        baker.make(VoorspelIntercept, _quantity=15)
        VoorspelInterceptCsvImporter("")._import_csv_reader([])
        mocked_truncate.assert_called_with()

    @mock.patch(
        "peoplemeasurement.csv_imports.voorspelintercept_csv_importer"
        ".VoorspelInterceptCsvImporter.create_model_instance"
    )
    def test_import_csv_reader_error(self, mocked_create):
        mocked_create.side_effect = Exception

        baker.make(VoorspelIntercept, _quantity=15)
        with pytest.raises(Exception):
            VoorspelInterceptCsvImporter("")._import_csv_reader(csv_reader=[1])
            assert VoorspelIntercept.objects.count() == 15

    def test_create_model_instance(self):
        row = dict(
            sensor="GKS-01-Kalverstraat",
            toepassings_kwartier_volgnummer="1",
            intercept_waarde="10.2515533281642",
        )

        obj = VoorspelInterceptCsvImporter("").create_model_instance(row)
        for key, value in row.items():
            expected_value = value
            if key == "toepassings_kwartier_volgnummer":
                expected_value = 1
            elif key == "intercept_waarde":
                expected_value = 10.2515533281642

            value = getattr(obj, key)
            assert (
                value == expected_value
            ), f"Expected Voorspelintercept.{key} to have value '{expected_value}' but is {value}"

    # def test_truncate(self):
    #     """
    #     That that we delete all service levels
    #     """
    #     baker.make(VoorspelIntercept, _quantity=13)
    #     VoorspelInterceptCsvImporter("path/to/file.csv")._truncate()
    #     assert VoorspelIntercept.objects.count() == 0
