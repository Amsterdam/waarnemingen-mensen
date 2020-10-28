import logging
import os

from django.core.management.base import BaseCommand
from django.db import transaction

from main import settings
from peoplemeasurement.csv_imports.sensor_csv_importer import SensorCsvImporter
from peoplemeasurement.csv_imports.servicelevel_csv_importer import ServicelevelCsvImporter

log = logging.getLogger(__name__)

IMPORTERS = {
    'sensors': SensorCsvImporter,
    'servicelevels': ServicelevelCsvImporter
}


@transaction.atomic
class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            'data_name',
            default=None,
            choices=['sensors', 'servicelevels'],
            help="Import the sensors or the servicelevels from the included csv."
        )

    def handle(self, *args, **options):
        log.info(f"Delete existing data from the {options['data_name']} and import new {options['data_name']} from csv")

        csv_file_path = os.path.join(
            settings.BASE_DIR,
            f"../../csv_imports/data/peoplemeasurement_{options['data_name']}.csv"
        )

        importer = IMPORTERS[options['data_name']](csv_file_path=csv_file_path, delimiter=";")
        num = importer.import_csv()

        log.info(f"Done. Imported {num} {options['data_name']}")
