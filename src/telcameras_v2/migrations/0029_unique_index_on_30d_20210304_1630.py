from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('telcameras_v2', '0028_redeploy_v8_views_20210303_1704'),
    ]

    operations = [
        migrations.RunSQL(
            sql='CREATE UNIQUE INDEX cmsa_15min_view_v8_realtime_predic_sensor_timestamp_rounded_idx ON cmsa_15min_view_v8_realtime_predict_30d_materialized USING btree (sensor, timestamp_rounded);',
            reverse_sql='DROP INDEX cmsa_15min_view_v8_realtime_predic_sensor_timestamp_rounded_idx;',
        )
    ]
