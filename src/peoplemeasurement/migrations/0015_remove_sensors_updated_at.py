# Generated by Django 2.2.16 on 2020-10-08 12:33

from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("peoplemeasurement", "0014_auto_20201008_1317"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="sensors",
            name="updated_at",
        ),
    ]
