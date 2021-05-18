from django.db import migrations, models

from telcameras_v2.view_definitions import get_view_strings


class Migration(migrations.Migration):

    dependencies = [
        ('telcameras_v2', '0030_deploy_v9_views_20210316_1040'),
        ('telcameras_v3', '0003_groupaggregate_count_scrambled')
    ]

    # This deploys the v9 view's which use the crambled count fields

    # NOTE: Simply create the view's in this specific order because of circular dependencies.
    # The materialized view depends on the view, and the realtime view depends on the materialized view.

    _VIEW_NAME = "cmsa_15min_view_v10"
    _view_strings = get_view_strings(_VIEW_NAME)

    _REALTIME_30D_VIEW_NAME = "cmsa_15min_view_v10_realtime_30d"
    _realtime_30d_view_strings = get_view_strings(_REALTIME_30D_VIEW_NAME)

    _PREDICT_VIEW_NAME = "cmsa_15min_view_v10_predict"
    _predict_view_strings = get_view_strings(_PREDICT_VIEW_NAME)

    _REALTIME_PREDICT_VIEW_NAME = "cmsa_15min_view_v10_realtime_predict"
    _realtime_predict_view_strings = get_view_strings(_REALTIME_PREDICT_VIEW_NAME)

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
            sql=_realtime_30d_view_strings['sql'],
            reverse_sql=_realtime_30d_view_strings['reverse_sql']
        ),
        migrations.RunSQL(
            sql=_realtime_30d_view_strings['sql_materialized'],
            reverse_sql=_realtime_30d_view_strings['reverse_sql_materialized']
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
            sql=_realtime_predict_view_strings['sql'],
            reverse_sql=_realtime_predict_view_strings['reverse_sql']
        ),

    ]
