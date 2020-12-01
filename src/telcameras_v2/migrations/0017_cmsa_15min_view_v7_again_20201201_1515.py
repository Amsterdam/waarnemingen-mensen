from django.db import migrations

from telcameras_v2.view_definitions import get_view_strings


class Migration(migrations.Migration):

    dependencies = [
        ('telcameras_v2', '0016_cmsa_15min_view_v7_realtime_predict_20201119_1728'),
    ]

    _VIEW_NAME = "cmsa_15min_view_v7"
    _view_strings = get_view_strings(_VIEW_NAME)

    _REALTIME_VIEW_NAME = "cmsa_15min_view_v7_realtime_predict"
    _realtime_view_strings = get_view_strings(_REALTIME_VIEW_NAME)

    operations = [
        # First remove the views
        migrations.RunSQL(
            sql=_realtime_view_strings['reverse_sql'],
            reverse_sql=_realtime_view_strings['sql']
        ),
        migrations.RunSQL(
            sql=_view_strings['reverse_sql_materialized'],
            reverse_sql=_view_strings['sql_materialized']
        ),
        migrations.RunSQL(
            sql=_view_strings['reverse_sql'],
            reverse_sql=_view_strings['sql']
        ),

        # And then recreate them again
        migrations.RunSQL(
            sql=_view_strings['sql'],
            reverse_sql=_view_strings['reverse_sql']
        ),
        migrations.RunSQL(
            sql=_view_strings['sql_materialized'],
            reverse_sql=_view_strings['reverse_sql_materialized']
        ),
        migrations.RunSQL(
            sql=_realtime_view_strings['sql'],
            reverse_sql=_realtime_view_strings['reverse_sql']
        ),
    ]
