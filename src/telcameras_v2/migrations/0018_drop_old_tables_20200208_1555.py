from django.db import migrations

TABLES = [
    "telcameras_v2_countaggregate",
    "telcameras_v2_personaggregate",
    "telcameras_v2_observation",
]


class Migration(migrations.Migration):
    dependencies = [
        ("telcameras_v2", "0017_cmsa_15min_view_v7_again_20201201_1515"),
    ]

    # Remove the old tables which are still present after the migration to timescaledb
    operations = []
    for table in TABLES:
        operations.append(migrations.RunSQL(sql=f"DROP TABLE {table}_old;"))
