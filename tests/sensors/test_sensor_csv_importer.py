import os
from unittest import mock

import pytest
from django.contrib.gis.geos import Point
from model_bakery import baker

from peoplemeasurement.models import Sensors
from peoplemeasurement.sensors.sensor_csv_importer import SensorCsvImporter


@pytest.mark.django_db
class TestSensorCsvImporter:
    @classmethod
    def setup_class(cls):
        cls.test_files_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "files"
        )

    def test_import_csv_reader(self):
        assert Sensors.objects.count() == 0
        test_csv = os.path.join(self.test_files_path, "cmsa_sensors_test.csv")
        SensorCsvImporter(test_csv).import_csv()
        assert Sensors.objects.count() == 33

    @mock.patch(
        "peoplemeasurement.sensors.sensor_csv_importer"
        ".SensorCsvImporter._truncate_sensors"
    )
    def test_import_csv_reader_truncate(self, mocked_truncate):
        baker.make(Sensors, _quantity=15)
        SensorCsvImporter("")._import_csv_reader([])
        mocked_truncate.assert_called_with()

    @mock.patch(
        "peoplemeasurement.sensors.sensor_csv_importer"
        ".SensorCsvImporter._create_sensor_for_row"
    )
    def test_import_csv_reader_error(self, mocked_create):
        mocked_create.side_effect = Exception

        baker.make(Sensors, _quantity=15)
        with pytest.raises(Exception):
            SensorCsvImporter("")._import_csv_reader(csv_reader=[1])
            assert Sensors.objects.count() == 15

    def test_create_sensor_for_row(self):
        row = dict(
            geom="0101000020E6100000749C363EEE961340CB918433AE2F4A40",
            objectnummer="GAWW-03",
            soort="2D sensor",
            voeding="Eiven voeding via lichtnet",
            rotatie="115",
            actief="Ja",
            privacyverklaring="",
            location_name="Stoofsteeg",
            width="2.68",
            gebiedstype="Red_light",
            gebied="Wallen",
        )

        sensor = SensorCsvImporter("")._create_sensor_for_row(row)
        for key, value in row.items():
            expected_value = value
            if key == "geom":
                expected_value = Point(x=4.8973932, y=52.3725037, srid=4326)
            elif key == "rotatie":
                expected_value = 115
            elif key == "width":
                expected_value = 2.68

            value = getattr(sensor, key)
            assert (
                value == expected_value
            ), f"Expected Sensor.{key} to have value '{expected_value}' but is {value}"

    def test_truncate_sensors(self):
        """
        That that we delete all sensors
        """
        baker.make(Sensors, _quantity=13)
        SensorCsvImporter("path/to/file.csv")._truncate_sensors()
        assert Sensors.objects.count() == 0
