# Generated by Django 2.2.3 on 2019-07-17 15:04

import django.contrib.postgres.fields.jsonb
from django.db import migrations, models


class Migration(migrations.Migration):
    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="PeopleMeasurement",
            fields=[
                ("id", models.UUIDField(primary_key=True, serialize=False)),
                ("version", models.CharField(max_length=10)),
                ("timestamp", models.DateTimeField(db_index=True)),
                ("sensor", models.CharField(max_length=255)),
                ("sensortype", models.CharField(max_length=255)),
                ("latitude", models.DecimalField(decimal_places=11, max_digits=14)),
                ("longitude", models.DecimalField(decimal_places=11, max_digits=14)),
                ("density", models.FloatField(null=True)),
                ("speed", models.FloatField(null=True)),
                ("count", models.IntegerField(null=True)),
                ("details", django.contrib.postgres.fields.jsonb.JSONField(null=True)),
            ],
        ),
    ]
