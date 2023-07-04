import django.utils.timezone
from django.db import migrations

import contrib.timescale.fields

TABLES = [
    "centralerekenapplicatie_v1_areametric",
    "centralerekenapplicatie_v1_linemetric",
    "centralerekenapplicatie_v1_linemetriccount",
]


class Migration(migrations.Migration):
    dependencies = [
        ("centralerekenapplicatie_v1", "0001_initial"),
    ]

    operations = [
        migrations.AlterField(
            model_name="areametric",
            name="timestamp",
            field=contrib.timescale.fields.TimescaleDateTimeField(
                default=django.utils.timezone.now, interval="1 day"
            ),
        ),
        migrations.AlterField(
            model_name="linemetric",
            name="timestamp",
            field=contrib.timescale.fields.TimescaleDateTimeField(
                default=django.utils.timezone.now, interval="1 day"
            ),
        ),
        migrations.AlterField(
            model_name="linemetriccount",
            name="line_metric_timestamp",
            field=contrib.timescale.fields.TimescaleDateTimeField(
                default=django.utils.timezone.now, interval="1 day"
            ),
        ),
    ]

    for table in TABLES:
        # Create new tables
        operations.append(
            migrations.RunSQL(
                sql=f"CREATE TABLE {table}_hypertable (LIKE {table} INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES);",
                reverse_sql=f"DROP TABLE {table}_hypertable;",
            )
        )

        # Drop pkey constrains that prevents hypertable creation
        operations.append(
            migrations.RunSQL(
                sql=f"ALTER TABLE {table}_hypertable DROP CONSTRAINT {table}_hypertable_pkey;"
            )
        )

        # Convert the new tables to hypertables
        timescale_field = (
            "line_metric_timestamp"
            if table == "centralerekenapplicatie_v1_linemetriccount"
            else "timestamp"
        )
        operations.append(
            migrations.RunSQL(
                sql=f"SELECT create_hypertable('{table}_hypertable', '{timescale_field}', chunk_time_interval => INTERVAL '1 day');"
            )
        )

        # NOTE: There is no need to import data from the existing tables to the new hypertables since the tables
        # are all new and empty

        # Rename old table to something else
        # These old tables will be removed later
        operations.append(
            migrations.RunSQL(sql=f"ALTER TABLE {table} RENAME TO {table}_old;")
        )

        # Rename hypertable to correct name
        operations.append(
            migrations.RunSQL(sql=f"ALTER TABLE {table}_hypertable RENAME TO {table};")
        )

    for table in reversed(TABLES):
        # Make sequences only dependent on the new tables. This to make it possible to remove the old tables
        operations.append(
            migrations.RunSQL(sql=f"ALTER SEQUENCE {table}_id_seq OWNED BY {table}.id;")
        )

        # Remove the old tables
        operations.append(migrations.RunSQL(sql=f"DROP TABLE {table}_old;"))
