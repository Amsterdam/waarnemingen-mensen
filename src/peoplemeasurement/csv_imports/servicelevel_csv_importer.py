import logging

from django.db import transaction, connection

from peoplemeasurement.models import Servicelevel
from peoplemeasurement.csv_imports.csv_importer import CsvImporter

logger = logging.getLogger(__name__)


class ServicelevelCsvImporter(CsvImporter):
    def _import_csv_reader(self, csv_reader) -> int:
        with transaction.atomic():
            if Servicelevel.objects.count() > 0:
                self._truncate_servicelevels()

            servicelevels = []
            for row in csv_reader:
                servicelevels.append(self._create_servicelevel_for_row(row))

            if servicelevels:
                Servicelevel.objects.bulk_create(servicelevels)

        return len(servicelevels)

    def _create_servicelevel_for_row(self, row):
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

    def _truncate_servicelevels(self):
        # using ignore so cmsa_1h_count_view_v1 reference will
        # not cause any issues. If deleting normally we'd get an error like so:
        # cannot drop table peoplemeasurement_servicelevel because other objects depend on it
        cursor = connection.cursor()
        cursor.execute("TRUNCATE TABLE peoplemeasurement_servicelevel;")
