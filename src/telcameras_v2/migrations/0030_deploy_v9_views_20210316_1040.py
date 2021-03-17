from django.db import migrations

from telcameras_v2.view_definitions import get_view_strings


class Migration(migrations.Migration):

    dependencies = [
        ('telcameras_v2', '0029_unique_index_on_30d_20210304_1630'),
        ('telcameras_v3', '0003_groupaggregate_count_scrambled')
    ]

    # This deploys the v9 view's which use the crambled count fields

    # NOTE: Simply create the view's in this specific order because of circular dependencies.
    # The materialized view depends on the view, and the realtime view depends on the materialized view.

    _VIEW_NAME = "cmsa_15min_view_v9"
    _view_strings = get_view_strings(_VIEW_NAME)

    _REALTIME_VIEW_NAME = "cmsa_15min_view_v9_realtime_predict"
    _realtime_view_strings = get_view_strings(_REALTIME_VIEW_NAME)

    _REALTIME_30D_VIEW_NAME = "cmsa_15min_view_v9_realtime_predict_30d"
    _realtime_30d_view_strings = get_view_strings(_REALTIME_30D_VIEW_NAME)

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
            sql=_realtime_view_strings['sql'],
            reverse_sql=_realtime_view_strings['reverse_sql']
        ),
        migrations.RunSQL(
            sql=_realtime_view_strings['sql_materialized'],
            reverse_sql=_realtime_view_strings['reverse_sql_materialized']
        ),
        migrations.RunSQL(
            sql=_realtime_30d_view_strings['sql'],
            reverse_sql=_realtime_30d_view_strings['reverse_sql']
        ),
        migrations.RunSQL(
            sql=_realtime_30d_view_strings['sql_materialized'],
            reverse_sql=_realtime_30d_view_strings['reverse_sql_materialized']
        ),
    ]
