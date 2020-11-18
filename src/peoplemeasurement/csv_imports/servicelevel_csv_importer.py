import logging

from django.db import transaction, connection

from peoplemeasurement.models import Servicelevel
from peoplemeasurement.csv_imports.csv_importer import CsvImporter

logger = logging.getLogger(__name__)


class ServicelevelCsvImporter(CsvImporter):
    model = Servicelevel

    def create_obj_dict_for_row(self, row):
        data = dict(
            type_parameter=row['type_parameter'],
            type_gebied=row['type_gebied'],
            type_tijd=row['type_tijd'],
            level_nr=self.to_int(row['level_nr']),
            level_label=row['level_label'],
            lowerlimit=self.to_float(row['lowerlimit']),
            upperlimit=self.to_float(row['upperlimit']),
        )

        return Servicelevel(**data)
