import django.utils.timezone
from django.db import migrations

import contrib.timescale.fields


class Migration(migrations.Migration):
    dependencies = [
        ("centralerekenapplicatie_v1", "0004_auto_20210526_1621"),
    ]

    operations = [
        migrations.AlterField(
            model_name="CountMetric",
            name="timestamp",
            field=contrib.timescale.fields.TimescaleDateTimeField(
                default=django.utils.timezone.now, interval="1 day"
            ),
        ),
    ]

    table = "centralerekenapplicatie_v1_countmetric"
    timescale_field = "timestamp"

    # Create new table
    operations.append(
        migrations.RunSQL(
            sql=f"CREATE TABLE {table}_hypertable (LIKE {table} INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES);",
            reverse_sql=f"DROP TABLE {table}_hypertable;",
        )
    )

    # Convert the new table to hypertables
    operations.append(
        migrations.RunSQL(
            sql=f"SELECT create_hypertable('{table}_hypertable', '{timescale_field}', chunk_time_interval => INTERVAL '1 day');"
        )
    )

    # NOTE: There is no need to import data from the existing tables to the new hypertables since the tables
    # are all new and empty

    # Rename old table to something else
    # These old table will be removed later
    operations.append(
        migrations.RunSQL(sql=f"ALTER TABLE {table} RENAME TO {table}_old;")
    )

    # Rename hypertable to correct name
    operations.append(
        migrations.RunSQL(sql=f"ALTER TABLE {table}_hypertable RENAME TO {table};")
    )

    # Make sequence only dependent on the new table. This to make it possible to remove the old tables.
    operations.append(
        migrations.RunSQL(sql=f"ALTER SEQUENCE {table}_id_seq OWNED BY {table}.id;")
    )

    # Remove the old table
    operations.append(migrations.RunSQL(sql=f"DROP TABLE {table}_old;"))
