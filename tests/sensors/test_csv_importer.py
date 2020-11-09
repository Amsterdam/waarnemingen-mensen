import os
from unittest import mock
from unittest.mock import mock_open

import pytest

from peoplemeasurement.csv_imports.csv_importer import CsvImporter


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
    @mock.patch("peoplemeasurement.csv_imports.csv_importer.csv")
    @mock.patch("peoplemeasurement.csv_imports.csv_importer.CsvImporter._import_csv_reader")
    def test_import_csv(
        self, mocked_import, mocked_csv, mocked_file
    ):
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
    @mock.patch("peoplemeasurement.csv_imports.csv_importer.csv")
    @mock.patch("peoplemeasurement.csv_imports.csv_importer.CsvImporter._import_csv_reader")
    def test_import_csv_value_error(
        self, mocked_import, mocked_csv, mocked_file
    ):
        mocked_import.return_value = 0

        with pytest.raises(ValueError):
            CsvImporter('filepath.csv').import_csv()

    @mock.patch("peoplemeasurement.csv_imports.csv_importer.CsvImporter.logger")
    def test_import_csv_file_not_found(self, mocked_logger):
        filepath = "non/existing/path/file.csv"
        importer = CsvImporter(filepath)
        result = importer.import_csv()
        assert result is None
        mocked_logger.warning.assert_called_with(
            f"CSV file for import ({'non/existing/path/file.csv'}) does not exist"
        )

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
    def test_to_float_invalid(self, input_str):
        """
        Test casting an invalid integer and assert that a ValueError is raised.
        """
        importer = CsvImporter(csv_file_path=None)
        with pytest.raises(ValueError):
            importer.to_float(input_str)
