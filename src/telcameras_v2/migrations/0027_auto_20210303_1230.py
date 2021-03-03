from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('telcameras_v2', '0026_redeploy_v8_realpre_and_add_realpre_30d_20210225_1537'),
    ]

    operations = [
        migrations.RunSQL(
            sql='CREATE UNIQUE INDEX cmsa_15min_view_v8_realtime_predic_sensor_timestamp_rounded_idx ON cmsa_15min_view_v8_realtime_predict_30d_materialized USING btree (sensor, timestamp_rounded);',
            reverse_sql='DROP INDEX cmsa_15min_view_v8_realtime_predic_sensor_timestamp_rounded_idx;',
        )
    ]
