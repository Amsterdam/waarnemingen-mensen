from django.db import migrations

from telcameras_v2.view_definitions import VIEW_STRINGS, get_view_strings


class Migration(migrations.Migration):

    dependencies = [
        ('telcameras_v2', '0020_cmsa_15min_view_v8_20210209_2016'),
    ]

    _VIEW_NAME = "cmsa_15min_view_v8"
    _view_strings = get_view_strings(VIEW_STRINGS, _VIEW_NAME)

    operations = [
        # First remove the views
        migrations.RunSQL(
            sql=_view_strings['reverse_sql_materialized'],
            reverse_sql=_view_strings['sql_materialized'],
        ),
        migrations.RunSQL(
            sql=_view_strings['reverse_sql'],
            reverse_sql=_view_strings['sql'],
        ),

        # And then redeploy it again
        migrations.RunSQL(
            sql=_view_strings['sql'],
            reverse_sql=_view_strings['reverse_sql']
        ),
        migrations.RunSQL(
            sql=_view_strings['sql_materialized'],
            reverse_sql=_view_strings['reverse_sql_materialized']
        ),
    ]
