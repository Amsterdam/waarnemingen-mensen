import logging

from django.db import transaction, connection

from peoplemeasurement.models import VoorspelCoefficient
from peoplemeasurement.csv_imports.csv_importer import CsvImporter

logger = logging.getLogger(__name__)


class VoorspelCoefficientCsvImporter(CsvImporter):
    model = VoorspelCoefficient

    def create_model_instance(self, row):
        data = dict(
            sensor=row['sensor'],
            bron_kwartier_volgnummer=self.to_int(row['bron_kwartier_volgnummer']),
            toepassings_kwartier_volgnummer=self.to_int(row['toepassings_kwartier_volgnummer']),
            coefficient_waarde=self.to_float(row['coefficient_waarde']),
        )

        return VoorspelCoefficient(**data)
