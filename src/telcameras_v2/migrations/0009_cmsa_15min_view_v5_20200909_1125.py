from django.db import migrations

from telcameras_v2.view_definitions import get_view_strings


class Migration(migrations.Migration):

    dependencies = [
        ('telcameras_v2', '0008_cmsa_15min_view_v4_20200902_1420'),
    ]

    # VIEW DESCRIPTION: This view only uses data from the telcameras_v2 from the time we actually have data, and
    # then disregards data from peoplemeasurement (v1)
    _VIEW_NAME = "cmsa_15min_view_v5"
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
