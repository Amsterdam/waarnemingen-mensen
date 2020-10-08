import datetime
import os
from unittest import mock
from unittest.mock import mock_open

import pytest
from django.db.models import DateField, IntegerField, BooleanField, FloatField

from peoplemeasurement.sensors.csv_importer import CsvImporter


class TestCsvImporter:
    @classmethod
    def setup_class(cls):
        cls.test_files_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "files"
        )

    def test_init(self):
        importer = CsvImporter("csv/file/path.csv", ",", "utf-8")
        assert importer.csv_file_path == "csv/file/path.csv"
        assert importer.delimiter == ","
        assert importer.encoding == "utf-8"

    def test_init_defaults(self):
        importer = CsvImporter(
            "csv/file/path2.csv",
        )
        assert importer.csv_file_path == "csv/file/path2.csv"
        assert importer.delimiter == ";"
        assert importer.encoding is None

    def test_abstract_method(self):
        """
        Test the abstract method of the CsvImporter
        and assert a NotImplementedError is raised.
        """
        csv_path = os.path.join(self.test_files_path, "empty.csv")
        with pytest.raises(NotImplementedError):
            importer = CsvImporter(csv_path)
            importer.import_csv()

    @mock.patch("builtins.open", new_callable=mock_open)
    @mock.patch("peoplemeasurement.sensors.csv_importer.csv")
    @mock.patch("peoplemeasurement.sensors.csv_importer.CsvImporter._get_encoding")
    @mock.patch("peoplemeasurement.sensors.csv_importer.CsvImporter._import_csv_reader")
    def test_import_csv(
        self, mocked_import, mocked_get_encoding, mocked_csv, mocked_file
    ):
        mocked_get_encoding.return_value = "test-encoding"
        mocked_csv.DictReader.return_value = "foobar"
        mocked_import.return_value = 99

        delimiter = "::"
        filepath = "path/to/file.csv"
        importer = CsvImporter(filepath, delimiter)
        result = importer.import_csv()

        assert result == 99
        mocked_csv.DictReader.assert_called_with(f=mocked_file(), delimiter=delimiter)
        mocked_import.assert_called_with("foobar")

    @mock.patch("builtins.open", new_callable=mock_open)
    @mock.patch("peoplemeasurement.sensors.csv_importer.csv")
    @mock.patch("peoplemeasurement.sensors.csv_importer.CsvImporter._import_csv_reader")
    def test_import_csv_value_error(
        self, mocked_import, mocked_csv, mocked_file
    ):
        mocked_import.return_value = 0

        with pytest.raises(ValueError):
            CsvImporter('filepath.csv').import_csv()

    @mock.patch("peoplemeasurement.sensors.csv_importer.CsvImporter.logger")
    def test_import_csv_file_not_found(self, mocked_logger):
        filepath = "non/existing/path/file.csv"
        importer = CsvImporter(filepath)
        result = importer.import_csv()
        assert result is None
        mocked_logger.warning.assert_called_with(
            f"CSV file for import ({'non/existing/path/file.csv'}) does not exist"
        )

    @pytest.mark.parametrize(
        "date_str,expected_result",
        [
            ("25-05-2019", datetime.datetime(2019, 5, 25).date()),
            ("31-12-2020", datetime.datetime(2020, 12, 31).date()),
        ],
    )
    def test_to_date_valid(self, date_str, expected_result):
        """
        Test casting a valid date and assert that
        the correct date representation is returned.
        """
        importer = CsvImporter(csv_file_path=None)
        result = importer.to_date(date_str)
        assert result == expected_result

    @pytest.mark.parametrize(
        "invalid_date_str",
        [
            "this is not a date",
            "03/05/2019",
            "2019-31-10",
            "05-25-2019",
        ],
    )
    def test_to_date_invalid(self, invalid_date_str):
        """
        Test casting an invalid data and assert that a ValueError is raised.
        """
        importer = CsvImporter(csv_file_path=None)
        with pytest.raises(ValueError):
            importer.to_date(invalid_date_str)

    @pytest.mark.parametrize(
        "null_str",
        [
            "NULL",
            "null",
        ],
    )
    def test_get_value_null(self, null_str):
        """
        Test the get value method and assert that 'NULL' returns None.
        """
        importer = CsvImporter(csv_file_path=None)
        result = importer.get_value(null_str)
        assert result is None, "Expected NULL to result in None"

    @pytest.mark.parametrize(
        "input_str,expected_output",
        [
            ("    lstrip", "lstrip"),
            ("rstrip    ", "rstrip"),
            ("   strip   ", "strip"),
        ],
    )
    def test_get_value_strip(self, input_str, expected_output):
        """
        Test the get value method and assert that left and right whitespace is removed.
        """
        importer = CsvImporter(csv_file_path=None)
        result = importer.get_value(input_str)
        assert result == expected_output

    @pytest.mark.parametrize(
        "input_str,expected_output",
        [
            ("1", 1),
            ("99", 99),
            ("123456", 123456),
            ("", None)
        ],
    )
    def test_to_int_valid(self, input_str, expected_output):
        """
        Test casting a valid integer and assert
        that the correct int representation is returned.
        """
        importer = CsvImporter(csv_file_path=None)
        result = importer.to_int(input_str)
        assert result == expected_output

    @pytest.mark.parametrize("input_str", ["x", "123abc", "abc123", "abc"])
    def test_to_int_invalid(self, input_str):
        """
        Test casting an invalid integer and assert that a ValueError is raised.
        """
        importer = CsvImporter(csv_file_path=None)
        with pytest.raises(ValueError):
            importer.to_int(input_str)

    @pytest.mark.parametrize(
        "input_str,expected_output",
        [
            ("1.1", 1.1),
            ("99.99", 99.99),
            ("123456", 123456.0),
            ("", None)
        ],
    )
    def test_to_float_valid(self, input_str, expected_output):
        """
        Test casting a valid float and assert
        that the correct int representation is returned.
        """
        importer = CsvImporter(csv_file_path=None)
        result = importer.to_float(input_str)
        assert result == expected_output

    @pytest.mark.parametrize("input_str", ["x", "123abc", "abc123", "abc"])
    def test_to_int_invalid(self, input_str):
        """
        Test casting an invalid integer and assert that a ValueError is raised.
        """
        importer = CsvImporter(csv_file_path=None)
        with pytest.raises(ValueError):
            importer.to_float(input_str)

    @pytest.mark.parametrize("bool_str", ["TRUE", "true", "True", "tRUe"])
    def test_to_boolean_valid(self, bool_str):
        """
        Test casting a valid boolean and assert
        that the correct int representation is returned.
        """
        importer = CsvImporter(csv_file_path=None)
        result = importer.to_boolean(bool_str)
        assert result is True

    @pytest.mark.parametrize("bool_str", ["false", "False", "FALSE", "1", "yes", "y", ""])
    def test_to_boolean_invalid(self, bool_str):
        """
        Test casting an invalid boolean and assert that a ValueError is raised.
        """
        importer = CsvImporter(csv_file_path=None)
        assert not importer.to_boolean(bool_str)

    @mock.patch("peoplemeasurement.sensors.csv_importer.CsvImporter.to_int")
    def test_cast_value_int(self, mocked_to_int):
        """
        Test casting an IntegerField and assert that type int is returned
        """
        mocked_to_int.return_value = 12345
        importer = CsvImporter(csv_file_path=None)
        result = importer.cast_value(IntegerField(), "6")
        mocked_to_int.assert_called_with("6")
        assert result == 12345

    @mock.patch("peoplemeasurement.sensors.csv_importer.CsvImporter.to_float")
    def test_cast_value_float(self, mocked_to_int):
        """
        Test casting an IntegerField and assert that type int is returned
        """
        mocked_to_int.return_value = 12345.678
        importer = CsvImporter(csv_file_path=None)
        result = importer.cast_value(FloatField(), "1.1")
        mocked_to_int.assert_called_with("1.1")
        assert result == 12345.678

    @mock.patch("peoplemeasurement.sensors.csv_importer.CsvImporter.to_date")
    def test_cast_value_date(self, mocked_to_date):
        """
        Test casting a DateField and assert that type date is returned
        """
        mocked_to_date.return_value = datetime.datetime(2019, 1, 1).date()
        importer = CsvImporter(csv_file_path=None)
        result = importer.cast_value(DateField(), "05-06-2019")
        mocked_to_date.assert_called_with("05-06-2019")
        assert result == mocked_to_date.return_value

    @mock.patch("peoplemeasurement.sensors.csv_importer.CsvImporter.to_boolean")
    def test_cast_value_boolean(self, mocked_to_boolean):
        """
        Test casting a DateField and assert that type date is returned
        """
        mocked_to_boolean.return_value = True
        importer = CsvImporter(csv_file_path=None)
        result = importer.cast_value(BooleanField(), "true")
        mocked_to_boolean.assert_called_with("true")
        assert result is True

    @mock.patch("peoplemeasurement.sensors.csv_importer.CsvImporter.get_value")
    def test_cast_value(self, mocked_get_value):
        mocked_get_value.return_value = "foobar"
        importer = CsvImporter(csv_file_path=None)
        result = importer.cast_value(None, "true")
        mocked_get_value.assert_called_with("true")
        assert result == "foobar"

    @mock.patch("peoplemeasurement.sensors.csv_importer.CsvImporter.to_date")
    def test_cast_value_exception(self, mocked_to_date):
        mocked_to_date.side_effect = ValueError
        importer = CsvImporter(csv_file_path=None)
        with pytest.raises(ValueError):
            importer.cast_value(DateField(), "value")

    def test_get_encoding(self):
        encoding = CsvImporter(
            csv_file_path="x.csv", encoding="foo-bar-baz"
        )._get_encoding()
        assert encoding == "foo-bar-baz"
