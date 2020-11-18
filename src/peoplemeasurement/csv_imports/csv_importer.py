import csv
import logging
from abc import ABC, abstractmethod
from django.db import transaction, connection


class CsvImporter(ABC):

    logger = logging.getLogger(__name__)

    @property
    @abstractmethod
    def model(self):
        """ The Django model to which this csv should be saved"""
        pass

    @abstractmethod
    def create_obj_dict_for_row(self, row) -> dict:
        """
        Convert a row to a dict resembling the model
        """
        pass

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
        with transaction.atomic():
            if self.model.objects.count() > 0:
                self._truncate()

            obj_dicts = []
            for row in csv_reader:
                obj_dicts.append(self.create_obj_dict_for_row(row))

            if obj_dicts:
                self.model.objects.bulk_create(obj_dicts)

        return len(obj_dicts)

    def _truncate(self):
        # using ignore so cmsa_1h_count_view_v1 reference will
        # not cause any issues. If deleting normally we'd get an error like so:
        # cannot drop table xxx_table because other objects depend on it
        cursor = connection.cursor()
        cursor.execute(f"TRUNCATE TABLE {self.model.objects.model._meta.db_table};")
