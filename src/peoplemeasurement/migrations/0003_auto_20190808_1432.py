# Generated by Django 2.2.4 on 2019-08-08 12:32

from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("peoplemeasurement", "0002_peoplemeasurementcsv_peoplemeasurementcsvtemp"),
    ]

    operations = [
        migrations.DeleteModel(
            name="PeopleMeasurementCSV",
        ),
        migrations.DeleteModel(
            name="PeopleMeasurementCSVTemp",
        ),
    ]
