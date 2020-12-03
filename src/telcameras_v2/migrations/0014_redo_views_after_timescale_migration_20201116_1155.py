from django.db import migrations

from telcameras_v2.view_definitions import get_view_strings


# These views need to be added again
VIEWS = [
    'cmsa_15min_view_v4',
    'cmsa_15min_view_v5',
    'cmsa_15min_view_v6',
    'cmsa_15min_view_v6_realtime',
]


class Migration(migrations.Migration):

    dependencies = [
        ('telcameras_v2', '0013_auto_20201111_1050'),
    ]
    # This removes the old tables that were left behind after the timescale migration
    # Like black lives, the order of the commands also matters.
    # The removed views will be add in subsequent migrations
    operations = [
        # Remove  dependent views
        migrations.RunSQL(sql=f"DROP MATERIALIZED VIEW cmsa_15min_view_v4_materialized;"),
        migrations.RunSQL(sql=f"DROP VIEW cmsa_15min_view_v4;"),
        migrations.RunSQL(sql=f"DROP MATERIALIZED VIEW cmsa_15min_view_v5_materialized;"),
        migrations.RunSQL(sql=f"DROP VIEW cmsa_15min_view_v5;"),
        migrations.RunSQL(sql=f"DROP VIEW cmsa_15min_view_v6_realtime;"),
        migrations.RunSQL(sql=f"DROP MATERIALIZED VIEW cmsa_15min_view_v6_materialized;"),
        migrations.RunSQL(sql=f"DROP VIEW cmsa_15min_view_v6;"),

        # Make sequences only dependent on the new tables. This to make it possible to remove the old tables later
        migrations.RunSQL(sql=f"ALTER SEQUENCE telcameras_v2_countaggregate_id_seq OWNED BY telcameras_v2_countaggregate.id;"),
        migrations.RunSQL(sql=f"ALTER SEQUENCE telcameras_v2_personaggregate_id_seq OWNED BY telcameras_v2_personaggregate.id;"),
        migrations.RunSQL(sql=f"ALTER SEQUENCE telcameras_v2_observation_id_seq OWNED BY telcameras_v2_observation.id;"),
    ]

    # Add the previously removed views and their accompanying materialized views again
    for view in VIEWS:
        view_strings = get_view_strings(view)

        operations.append(
            migrations.RunSQL(
                sql=view_strings['sql'],
                reverse_sql=view_strings['reverse_sql']
            )
        )
        if view != 'cmsa_15min_view_v6_realtime':
            operations.append(
                migrations.RunSQL(
                    sql=view_strings['sql_materialized'],
                    reverse_sql=view_strings['reverse_sql_materialized']
                ),
            )
