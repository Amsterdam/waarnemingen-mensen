# Generated by Django 3.0.11 on 2021-01-27 11:38

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):
    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="AreaMetric",
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
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("message_id", models.TextField()),
                ("sensor", models.CharField(max_length=255)),
                ("timestamp", models.DateTimeField()),
                ("original_id", models.CharField(max_length=255)),
                ("admin_id", models.IntegerField()),
                ("area", models.FloatField()),
                ("count", models.IntegerField()),
                ("density", models.FloatField()),
                ("total_distance", models.FloatField()),
                ("total_time", models.FloatField()),
                ("speed", models.FloatField(null=True)),
            ],
            options={
                "abstract": False,
            },
        ),
        migrations.CreateModel(
            name="LineMetric",
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
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("message_id", models.TextField()),
                ("sensor", models.CharField(max_length=255)),
                ("timestamp", models.DateTimeField()),
                ("original_id", models.CharField(max_length=255)),
                ("admin_id", models.IntegerField()),
            ],
            options={
                "abstract": False,
            },
        ),
        migrations.CreateModel(
            name="LineMetricCount",
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
                ("line_metric_timestamp", models.DateTimeField()),
                ("azimuth", models.FloatField()),
                ("count", models.IntegerField()),
                (
                    "line_metric",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        to="centralerekenapplicatie_v1.LineMetric",
                    ),
                ),
            ],
        ),
    ]
