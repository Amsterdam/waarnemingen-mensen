import logging

from django.db import transaction, connection

from peoplemeasurement.models import Sensors
from peoplemeasurement.sensors.csv_importer import CsvImporter

logger = logging.getLogger(__name__)


class SensorCsvImporter(CsvImporter):

    def _import_csv_reader(self, csv_reader) -> int:
        with transaction.atomic():
            if Sensors.objects.count() > 0:
                self._truncate_sensors()

            sensors = []
            for row in csv_reader:
                sensors.append(self._create_sensor_for_row(row))

            if sensors:
                Sensors.objects.bulk_create(sensors)

        return len(sensors)

    def _create_sensor_for_row(self, row):
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

    def _truncate_sensors(self):
        # using ignore so cmsa_1h_count_view_v1 reference will
        # not cause any issues. If deleting normally we'd get an error like so:
        # cannot drop table peoplemeasurement_sensors because other objects depend on it
        cursor = connection.cursor()
        cursor.execute("TRUNCATE TABLE peoplemeasurement_sensors;")
