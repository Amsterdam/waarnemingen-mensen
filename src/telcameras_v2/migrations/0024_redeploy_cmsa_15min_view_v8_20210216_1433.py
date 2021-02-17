from django.db import migrations

from telcameras_v2.view_definitions import get_view_strings


class Migration(migrations.Migration):

    dependencies = [
        ('telcameras_v2', '0023_drop_old_views_20210209_1153'),
    ]

    # NOTE: Simply remove the views and redeploy them. Because the materialized view
    # dependes on the view, and the realtime view dependes on the materialized view, I need to remove all
    # three of them, and then recreate them again in reversing order.

    _VIEW_NAME = "cmsa_15min_view_v8"
    _view_strings = get_view_strings(_VIEW_NAME)

    _REALTIME_VIEW_NAME = "cmsa_15min_view_v8_realtime_predict"
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
        # And also make a materialized version of the predict view which we can refresh every minute
        migrations.RunSQL(
            sql=_realtime_view_strings['sql_materialized'],
            reverse_sql=_realtime_view_strings['reverse_sql_materialized']
        ),
    ]
