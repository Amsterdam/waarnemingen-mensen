import csv
import logging


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
            with open(self.csv_file_path, 'r', encoding=None) as csv_file:
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
        return value.strip() if value else None

    def to_int(self, value):
        value = self.get_value(value)
        # We sometimes get values like `15.0`, which we want to parse to the int 15
        # For this reason we first parse to float and then to int
        return int(float(value)) if value else None

    def to_float(self, value):
        value = self.get_value(value)
        return float(value) if value else None

    def _import_csv_reader(self, csv_reader) -> int:
        """
        Do the actual csv import given
        :param csv_reader: csv.DictReader
        :return: number of imported rows
        """
        raise NotImplementedError()

