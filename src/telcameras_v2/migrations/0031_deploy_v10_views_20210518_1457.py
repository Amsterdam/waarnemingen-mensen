from django.db import migrations, models

from telcameras_v2.view_definitions import VIEW_STRINGS, get_view_strings


class Migration(migrations.Migration):

    dependencies = [
        ('telcameras_v2', '0030_deploy_v9_views_20210316_1040')
    ]

    # NOTE: Simply create the view's in this specific order because of circular dependencies.
    # The materialized view depends on the view, and the realtime view depends on the materialized view.

    _VIEW_NAME = "cmsa_15min_view_v10"
    _view_strings = get_view_strings(VIEW_STRINGS, _VIEW_NAME, indexes=[('sensor', 'timestamp_rounded')])

    _REALTIME_30D_VIEW_NAME = "cmsa_15min_view_v10_realtime_30d"
    _realtime_30d_view_strings = get_view_strings(VIEW_STRINGS, _REALTIME_30D_VIEW_NAME, indexes=[('sensor', 'timestamp_rounded')])

    _PREDICT_VIEW_NAME = "cmsa_15min_view_v10_predict"
    _predict_view_strings = get_view_strings(VIEW_STRINGS, _PREDICT_VIEW_NAME, indexes=[('sensor', 'timestamp_rounded')])

    _REALTIME_PREDICT_VIEW_NAME = "cmsa_15min_view_v10_realtime_predict"
    _realtime_predict_view_strings = get_view_strings(VIEW_STRINGS, _REALTIME_PREDICT_VIEW_NAME)

    operations = [
        # Create the views
        migrations.RunSQL(
            sql=_view_strings['sql'],
            reverse_sql=_view_strings['reverse_sql']
        ),
        migrations.RunSQL(
            sql=_view_strings['sql_materialized'],
            reverse_sql=_view_strings['reverse_sql_materialized']
        ),
        migrations.RunSQL(
            sql=_view_strings['indexes'][0]
        ),

        migrations.RunSQL(
            sql=_realtime_30d_view_strings['sql'],
            reverse_sql=_realtime_30d_view_strings['reverse_sql']
        ),
        migrations.RunSQL(
            sql=_realtime_30d_view_strings['sql_materialized'],
            reverse_sql=_realtime_30d_view_strings['reverse_sql_materialized']
        ),
        migrations.RunSQL(
            sql=_realtime_30d_view_strings['indexes'][0]
        ),

        migrations.RunSQL(
            sql=_predict_view_strings['sql'],
            reverse_sql=_predict_view_strings['reverse_sql']
        ),
        migrations.RunSQL(
            sql=_predict_view_strings['sql_materialized'],
            reverse_sql=_predict_view_strings['reverse_sql_materialized']
        ),
        migrations.RunSQL(
            sql=_predict_view_strings['indexes'][0]
        ),

        migrations.RunSQL(
            sql=_realtime_predict_view_strings['sql'],
            reverse_sql=_realtime_predict_view_strings['reverse_sql']
        ),

    ]
