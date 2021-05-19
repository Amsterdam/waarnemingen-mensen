from django.db import migrations

from telcameras_v2.view_definitions import VIEW_STRINGS, get_view_strings


class Migration(migrations.Migration):

    dependencies = [
        ('telcameras_v2', '0015_cmsa_15min_view_v7_20201117_1514'),
    ]

    _VIEW_NAME = "cmsa_15min_view_v7_realtime_predict"
    _view_strings = get_view_strings(VIEW_STRINGS, _VIEW_NAME)


    operations = [
        migrations.RunSQL(
            sql=_view_strings['sql'],
            reverse_sql=_view_strings['reverse_sql']
        ),
    ]
