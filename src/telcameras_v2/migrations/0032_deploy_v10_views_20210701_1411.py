from django.db import migrations, models

from telcameras_v2.view_definitions import VIEW_STRINGS, get_view_strings


class Migration(migrations.Migration):
    dependencies = [
        ("telcameras_v2", "0031_redeploy_v8_views_20210607_1834"),
    ]

    # This script deploys the table 'peoplemeasurement_v1_data' and 'cmsa_15min_view_v10' views which contain a new prediction method and performance improvements
    # For more information about the 'peoplemeasurement_v1_data', see the peoplemeasurement models defintions file

    # NOTE: Simply create the view's in this specific order because of circular dependencies.
    # The materialized view depends on the view, and the realtime view depends on the materialized view.

    _V1_DATA_VIEW_NAME = "peoplemeasurement_v1_data"
    _v1_data_view_strings = get_view_strings(VIEW_STRINGS, _V1_DATA_VIEW_NAME)

    _VIEW_NAME = "cmsa_15min_view_v10"
    _view_strings = get_view_strings(
        VIEW_STRINGS, _VIEW_NAME, indexes=[("sensor", "timestamp_rounded")]
    )

    _REALTIME_VIEW_NAME = "cmsa_15min_view_v10_realtime"
    _realtime_view_strings = get_view_strings(
        VIEW_STRINGS, _REALTIME_VIEW_NAME, indexes=[("sensor", "timestamp_rounded")]
    )

    _PREDICT_VIEW_NAME = "cmsa_15min_view_v10_predict"
    _predict_view_strings = get_view_strings(VIEW_STRINGS, _PREDICT_VIEW_NAME)

    _REALTIME_PREDICT_VIEW_NAME = "cmsa_15min_view_v10_realtime_predict"
    _realtime_predict_view_strings = get_view_strings(
        VIEW_STRINGS, _REALTIME_PREDICT_VIEW_NAME
    )

    operations = [
        # Create the views
        migrations.RunSQL(
            sql=_v1_data_view_strings["sql"],
            reverse_sql=_v1_data_view_strings["reverse_sql"],
        ),
        migrations.RunSQL(
            sql=_view_strings["sql"], reverse_sql=_view_strings["reverse_sql"]
        ),
        migrations.RunSQL(
            sql=_view_strings["sql_materialized"],
            reverse_sql=_view_strings["reverse_sql_materialized"],
        ),
        migrations.RunSQL(sql=_view_strings["indexes"][0]),
        migrations.RunSQL(
            sql=_realtime_view_strings["sql"],
            reverse_sql=_realtime_view_strings["reverse_sql"],
        ),
        migrations.RunSQL(
            sql=_realtime_view_strings["sql_materialized"],
            reverse_sql=_realtime_view_strings["reverse_sql_materialized"],
        ),
        migrations.RunSQL(sql=_realtime_view_strings["indexes"][0]),
        migrations.RunSQL(
            sql=_predict_view_strings["sql"],
            reverse_sql=_predict_view_strings["reverse_sql"],
        ),
        migrations.RunSQL(
            sql=_realtime_predict_view_strings["sql"],
            reverse_sql=_realtime_predict_view_strings["reverse_sql"],
        ),
    ]
