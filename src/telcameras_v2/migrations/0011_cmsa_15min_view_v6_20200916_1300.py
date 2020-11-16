from django.db import migrations

from telcameras_v2.view_definitions import get_view_strings


class Migration(migrations.Migration):

    dependencies = [
        ('telcameras_v2', '0010_auto_20200911_1519'),
    ]

    # VIEW DESCRIPTION: This view only uses data from the telcameras_v2 from the time we actually have data, and
    # then disregards data from peoplemeasurement (v1)
    _VIEW_NAME = "cmsa_15min_view_v6"
    _view_strings = get_view_strings(_VIEW_NAME)

    operations = [
        migrations.RunSQL(
            sql=_view_strings['sql'],
            reverse_sql=_view_strings['sql']
        ),
        migrations.RunSQL(
            sql=_view_strings['sql_materialized'],
            reverse_sql=_view_strings['reverse_sql_materialized']
        ),
    ]
