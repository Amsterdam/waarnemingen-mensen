# Generated by Django 3.2.1 on 2021-05-12 11:26

import django.contrib.gis.db.models.fields
import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("peoplemeasurement", "0017_voorspelcoefficient_voorspelintercept"),
    ]

    operations = [
        migrations.CreateModel(
            name="Line",
            fields=[
                (
                    "id",
                    models.AutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("name", models.CharField(max_length=255)),
                (
                    "geom",
                    django.contrib.gis.db.models.fields.LineStringField(srid=4326),
                ),
                ("azimuth", models.FloatField()),
                (
                    "sensor",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        to="peoplemeasurement.sensors",
                    ),
                ),
            ],
        ),
        migrations.CreateModel(
            name="Area",
            fields=[
                (
                    "id",
                    models.AutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("name", models.CharField(max_length=255)),
                ("geom", django.contrib.gis.db.models.fields.PolygonField(srid=4326)),
                ("area", models.IntegerField()),
                (
                    "sensor",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        to="peoplemeasurement.sensors",
                    ),
                ),
            ],
        ),
    ]
