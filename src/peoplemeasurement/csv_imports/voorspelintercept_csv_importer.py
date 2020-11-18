import logging

from django.db import transaction, connection

from peoplemeasurement.models import VoorspelIntercept
from peoplemeasurement.csv_imports.csv_importer import CsvImporter

logger = logging.getLogger(__name__)


class VoorspelInterceptCsvImporter(CsvImporter):
    model = VoorspelIntercept

    def create_obj_dict_for_row(self, row):
        data = dict(
            sensor=row['sensor'],
            toepassings_kwartier_volgnummer=self.to_int(row['toepassings_kwartier_volgnummer']),
            intercept_waarde=self.to_float(row['intercept_waarde']),
        )

        return VoorspelIntercept(**data)
