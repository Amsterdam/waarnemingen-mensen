import logging

from django.db import transaction, connection

from peoplemeasurement.models import Sensors
from peoplemeasurement.csv_imports.csv_importer import CsvImporter

logger = logging.getLogger(__name__)


class SensorCsvImporter(CsvImporter):
    model = Sensors

    def create_model_instance(self, row):
        data = dict(
            geom=row['geom'],
            objectnummer=row['objectnummer'],
            soort=row['soort'],
            voeding=row['voeding'],
            rotatie=self.to_int(row['rotatie']),
            actief=row['actief'],
            privacyverklaring=row['privacyverklaring'],
            location_name=row['location_name'],
            width=self.to_float(row['width']),
            gebiedstype=row['gebiedstype'],
            gebied=row['gebied'],
        )

        return Sensors(**data)
