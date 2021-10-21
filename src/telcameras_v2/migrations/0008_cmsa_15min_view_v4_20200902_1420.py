from django.db import migrations

from telcameras_v2.view_definitions import VIEW_STRINGS, get_view_strings


class Migration(migrations.Migration):

    dependencies = [
        ('telcameras_v2', '0007_auto_20200803_1902'),
        ('peoplemeasurement', '0002_peoplemeasurementcsv_peoplemeasurementcsvtemp'),
    ]

    # VIEW DESCRIPTION: This view combines the data from the peoplemeasurement data and the telcameras_v2 data
    # into a union view.
    _VIEW_NAME = "cmsa_15min_view_v4"
    _view_strings = get_view_strings(VIEW_STRINGS, _VIEW_NAME)

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
