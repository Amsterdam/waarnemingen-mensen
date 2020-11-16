from django.db import migrations

from telcameras_v2.view_definitions import get_view_strings


TABLES = [
    'telcameras_v2_countaggregate',
    'telcameras_v2_personaggregate',
    'telcameras_v2_observation',
]


class Migration(migrations.Migration):

    dependencies = [
        ('telcameras_v2', '0014_redo_views_after_timescale_migration_20201116_1155'),
    ]

    # Remove the old tables which are still present after the migration to timescaledb
    operations = []
    for table in TABLES:
        operations.append(migrations.RunSQL(sql=f"DROP TABLE {table}_old;"))
