from django.core.management import call_command
from django.db import migrations


def createcachetable(apps, schema_editor):
    call_command("createcachetable")


class Migration(migrations.Migration):
    dependencies = [
        ("peoplemeasurement", "0019_sensors_gid"),
    ]

    operations = [
        migrations.RunPython(createcachetable),
    ]
