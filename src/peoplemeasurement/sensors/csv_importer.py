import csv
import datetime
import logging

from chardet import UniversalDetector
from django.db.models import BooleanField, DateField, IntegerField, FloatField


class CsvImporter:

    logger = logging.getLogger(__name__)

    def __init__(self, csv_file_path, delimiter=";", encoding=None):
        """
        :param csv_file_path: path to the csv file
        :param delimiter: csv delimiter, default ";"
        :param encoding: csv file encoding, set to None to automatically detect using Chardet library
        """
        self.csv_file_path = csv_file_path
        self.delimiter = delimiter
        self.encoding = encoding

    def import_csv(self):
        """
        Import a CSV line by line.
        :return: The number of imported rows from the csv
        """
        try:
            encoding = self._get_encoding()
            with open(self.csv_file_path, 'r', encoding=encoding) as csv_file:
                csv_reader = csv.DictReader(f=csv_file, delimiter=self.delimiter)

                num_imported_rows = self._import_csv_reader(csv_reader)
                if not num_imported_rows:
                    raise ValueError("CSV import failed: no data imported")

                return num_imported_rows

        except FileNotFoundError:
            # do nothing if the file does not exist
            self.logger.warning(f"CSV file for import ({self.csv_file_path}) does not exist")
            return

    def get_value(self, value):
        value = value.strip() if value else None
        if value is None or value.upper() == 'NULL':
            return None
        return value

    def to_date(self, value):
        value = self.get_value(value)
        return datetime.datetime.strptime(value, '%d-%m-%Y').date() if value else None

    def to_int(self, value):
        value = self.get_value(value)
        return int(value) if value else None

    def to_float(self, value):
        value = self.get_value(value)
        return float(value) if value else None

    def to_boolean(self, value):
        value = self.get_value(value)
        return value.upper() == 'TRUE' if value else False

    def cast_value(self, field, value):
        try:
            if isinstance(field, DateField):
                return self.to_date(value)
            elif isinstance(field, IntegerField):
                return self.to_int(value)
            elif isinstance(field, FloatField):
                return self.to_float(value)
            elif isinstance(field, BooleanField):
                return self.to_boolean(value)
            return self.get_value(value)
        except ValueError:
            self.logger.exception(f"Failed casting field '{field}'")
            raise

    def _import_csv_reader(self, csv_reader) -> int:
        """
        Do the actual csv import given
        :param csv_reader: csv.DictReader
        :return: number of imported rows
        """
        raise NotImplementedError()

    def _get_encoding(self):
        if self.encoding:
            return self.encoding

        detector = UniversalDetector()
        detector.reset()
        for line in open(self.csv_file_path, 'rb'):
            detector.feed(line)
            if detector.done:
                break
        detector.close()

        encoding = detector.result['encoding']
        confidence = detector.result['confidence']
        self.logger.info(f"Determined encoding of file {self.csv_file_path}: "
                         f"'{encoding}', with {confidence} confidence")
        return encoding
