import logging
import os

from django.core.management.base import BaseCommand
from django.db import transaction

from main import settings
from peoplemeasurement.sensors.sensor_csv_importer import SensorCsvImporter

log = logging.getLogger(__name__)


@transaction.atomic
class Command(BaseCommand):
    def handle(self, *args, **options):
        log.info(f"Delete existing sensors and import new sensors from csv")

        csv_file_path = os.path.join(
            settings.BASE_DIR, "peoplemeasurement/sensors/data/cmsa_sensors.csv"
        )
        importer = SensorCsvImporter(csv_file_path=csv_file_path, delimiter=";")
        num_sensors = importer.import_csv()

        log.info(f"Done. Imported {num_sensors} sensors")
