# Generated by Django 3.2.4 on 2021-08-16 15:34

from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("peoplemeasurement", "0024_alter_sensors_gid"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="sensors",
            name="drop_incoming_data",
        ),
    ]
