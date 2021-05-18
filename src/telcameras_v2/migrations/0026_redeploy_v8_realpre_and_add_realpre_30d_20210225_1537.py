from django.db import migrations

from telcameras_v2.view_definitions import VIEW_STRINGS, get_view_strings


class Migration(migrations.Migration):

    dependencies = [
        ('telcameras_v2', '0025_scramble_counts_20210120_1147'),
    ]

    # NOTE: Here we remove one views and redeploy it. Because the realtime
    # view dependes on the materialized view, I need to remove both and then
    # recreate them again in reversing order.

    _VIEW_NAME = "cmsa_15min_view_v8_realtime_predict"
    _view_strings = get_view_strings(VIEW_STRINGS, _VIEW_NAME)

    _30D_VIEW_NAME = "cmsa_15min_view_v8_realtime_predict_30d"
    _30d_view_strings = get_view_strings(VIEW_STRINGS, _30D_VIEW_NAME)

    operations = [
        # Deploy the new 30d view
        migrations.RunSQL(
            sql=_30d_view_strings['sql'],
            reverse_sql=_30d_view_strings['reverse_sql']
        ),
        migrations.RunSQL(
            sql=_30d_view_strings['sql_materialized'],
            reverse_sql=_30d_view_strings['reverse_sql_materialized']
        ),

        # Remove the existing 1d view
        migrations.RunSQL(
            sql=_view_strings['reverse_sql_materialized'],
            reverse_sql=_view_strings['sql_materialized']
        ),
        migrations.RunSQL(
            sql=_view_strings['reverse_sql'],
            reverse_sql=_view_strings['sql']
        ),

        # Then redeploy the 1d view
        migrations.RunSQL(
            sql=_view_strings['sql'],
            reverse_sql=_view_strings['reverse_sql']
        ),
        migrations.RunSQL(
            sql=_view_strings['sql_materialized'],
            reverse_sql=_view_strings['reverse_sql_materialized']
        ),
    ]
