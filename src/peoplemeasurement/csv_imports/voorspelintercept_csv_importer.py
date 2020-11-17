import logging

from django.db import transaction, connection

from peoplemeasurement.models import VoorspelIntercept
from peoplemeasurement.csv_imports.csv_importer import CsvImporter

logger = logging.getLogger(__name__)


class VoorspelInterceptCsvImporter(CsvImporter):
    def _import_csv_reader(self, csv_reader) -> int:
        with transaction.atomic():
            if VoorspelIntercept.objects.count() > 0:
                self._truncate()

            obj_dicts = []
            for row in csv_reader:
                obj_dicts.append(self._create_obj_dict_for_row(row))

            if obj_dicts:
                VoorspelIntercept.objects.bulk_create(obj_dicts)

        return len(obj_dicts)

    def _create_obj_dict_for_row(self, row):
        # sensor;toepassings_kwartier_volgnummer;intercept_waarde
        # GKS-01-Kalverstraat;1;10.2515533281642
        data = dict(
            sensor=row['sensor'],
            toepassings_kwartier_volgnummer=self.to_int(row['toepassings_kwartier_volgnummer']),
            intercept_waarde=self.to_float(row['intercept_waarde']),
        )

        return VoorspelIntercept(**data)

    def _truncate(self):
        # using ignore so cmsa_1h_count_view_v1 reference will
        # not cause any issues. If deleting normally we'd get an error like so:
        # cannot drop table peoplemeasurement_servicelevel because other objects depend on it
        cursor = connection.cursor()
        cursor.execute("TRUNCATE TABLE peoplemeasurement_voorspelintercept;")
