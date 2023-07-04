from django.db import migrations

from telcameras_v2.view_definitions import VIEW_STRINGS, get_view_strings


class Migration(migrations.Migration):
    dependencies = [
        ("telcameras_v2", "0021_redeploy_cmsa_15min_view_v8_20210211_1344"),
    ]

    _VIEW_NAME = "cmsa_15min_view_v8_realtime_predict"
    _view_strings = get_view_strings(VIEW_STRINGS, _VIEW_NAME)

    operations = [
        migrations.RunSQL(
            sql=_view_strings["sql"], reverse_sql=_view_strings["reverse_sql"]
        ),
        migrations.RunSQL(
            sql=_view_strings["sql_materialized"],
            reverse_sql=_view_strings["reverse_sql_materialized"],
        ),
    ]
