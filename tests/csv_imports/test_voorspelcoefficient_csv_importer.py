import os
from unittest import mock

import pytest
from model_bakery import baker

from peoplemeasurement.models import VoorspelCoefficient
from peoplemeasurement.csv_imports.voorspelcoefficient_csv_importer import VoorspelCoefficientCsvImporter


@pytest.mark.django_db
class TestVoorspelCoefficientCsvImporter:
    @classmethod
    def setup_class(cls):
        cls.test_files_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "files"
        )

    def test_import_csv_reader(self):
        assert VoorspelCoefficient.objects.count() == 0
        test_csv = os.path.join(self.test_files_path, "peoplemeasurement_voorspelcoefficients_test.csv")
        VoorspelCoefficientCsvImporter(test_csv).import_csv()
        assert VoorspelCoefficient.objects.count() == 448

    @mock.patch(
        "peoplemeasurement.csv_imports.voorspelcoefficient_csv_importer"
        ".VoorspelCoefficientCsvImporter._truncate"
    )
    def test_import_csv_reader_truncate(self, mocked_truncate):
        baker.make(VoorspelCoefficient, _quantity=15)
        VoorspelCoefficientCsvImporter("")._import_csv_reader([])
        mocked_truncate.assert_called_with()

    @mock.patch(
        "peoplemeasurement.csv_imports.voorspelcoefficient_csv_importer"
        ".VoorspelCoefficientCsvImporter.create_model_instance"
    )
    def test_import_csv_reader_error(self, mocked_create):
        mocked_create.side_effect = Exception

        baker.make(VoorspelCoefficient, _quantity=15)
        with pytest.raises(Exception):
            VoorspelCoefficientCsvImporter("")._import_csv_reader(csv_reader=[1])
            assert VoorspelCoefficient.objects.count() == 15

    def test_create_model_instance(self):
        row = dict(
            sensor="GKS-01-Kalverstraat",
            bron_kwartier_volgnummer="1",
            toepassings_kwartier_volgnummer="2",
            coefficient_waarde="0.779513231347418",
        )

        obj = VoorspelCoefficientCsvImporter("").create_model_instance(row)
        for key, value in row.items():
            expected_value = value
            if key == "bron_kwartier_volgnummer":
                expected_value = 1
            elif key == "toepassings_kwartier_volgnummer":
                expected_value = 2
            elif key == "coefficient_waarde":
                expected_value = 0.779513231347418

            value = getattr(obj, key)
            assert (
                value == expected_value
            ), f"Expected Voorspelcoefficient.{key} to have value '{expected_value}' but is {value}"

    # def test_truncate(self):
    #     """
    #     That that we delete all service levels
    #     """
    #     baker.make(VoorspelCoefficient, _quantity=13)
    #     VoorspelCoefficientCsvImporter("path/to/file.csv", VoorspelCoefficient)._truncate()
    #     assert VoorspelCoefficient.objects.count() == 0
