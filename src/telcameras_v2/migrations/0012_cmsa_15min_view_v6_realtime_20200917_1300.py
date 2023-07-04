from django.db import migrations

from telcameras_v2.view_definitions import VIEW_STRINGS, get_view_strings


class Migration(migrations.Migration):
    dependencies = [
        ("telcameras_v2", "0011_cmsa_15min_view_v6_20200916_1300"),
    ]

    # VIEW DESCRIPTION: This view combines the data from the materialized view v6 and combines it with the realtime data
    _VIEW_NAME = "cmsa_15min_view_v6_realtime"
    _view_strings = get_view_strings(VIEW_STRINGS, _VIEW_NAME)

    operations = [
        migrations.RunSQL(
            sql=_view_strings["sql"], reverse_sql=_view_strings["reverse_sql"]
        )
    ]
