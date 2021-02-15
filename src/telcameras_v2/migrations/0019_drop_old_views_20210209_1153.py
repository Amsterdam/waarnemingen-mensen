from django.db import migrations

VIEWS = [
    'cmsa_1h_count_view_v1',
    'cmsa_15min',
    'cmsa_15min_view_v2',
    'cmsa_15min_view_v3',
    'cmsa_15min_view_v4',
    'cmsa_15min_view_v5',
    'cmsa_15min_view_v6_realtime',
    'cmsa_15min_view_v6',
]


class Migration(migrations.Migration):

    dependencies = [
        ('telcameras_v2', '0018_drop_old_tables_20200208_1555'),
    ]

    # Remove the old views which aren't used anymore
    operations = []
    for view in VIEWS:
        if view != 'cmsa_15min_view_v6_realtime':
            operations.append(migrations.RunSQL(sql=f"DROP MATERIALIZED VIEW {view}_materialized;"))
        operations.append(migrations.RunSQL(sql=f"DROP VIEW {view};"))
