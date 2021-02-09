from django.db import migrations

from telcameras_v2.view_definitions import get_view_strings


class Migration(migrations.Migration):

    dependencies = [
        ('telcameras_v2', '0018_drop_old_tables_20200208_1555'),
        ('telcameras_v3', '0002_timescale_20201214_1412'),
    ]

    _VIEW_NAME = "cmsa_15min_view_v8"
    _view_strings = get_view_strings(_VIEW_NAME)

    operations = [
        migrations.RunSQL(
            sql=_view_strings['sql'],
            reverse_sql=_view_strings['reverse_sql']
        ),
        migrations.RunSQL(
            sql=_view_strings['sql_materialized'],
            reverse_sql=_view_strings['reverse_sql_materialized']
        ),
    ]
