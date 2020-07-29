from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('peoplemeasurement', '0009_sensors'),
    ]

    _VIEW_NAME = "cmsa_1h_count_view_v1"

    sql = f"""
CREATE VIEW {_VIEW_NAME} AS
SELECT sensor, location_name, date_trunc('hour', timestamp_rounded) as datum_uur, sum(total_count) as aantal_passanten
FROM cmsa_15min_view_v2 v
JOIN peoplemeasurement_sensors s on  s.objectnummer=v.sensor
WHERE timestamp_rounded > to_date('2019-01-01','YYYY-MM-DD')
GROUP BY sensor, location_name, date_trunc('hour', timestamp_rounded)
;
"""

    reverse_sql = f"DROP VIEW IF EXISTS {_VIEW_NAME};"

    sql_materialized = f"""
    CREATE MATERIALIZED VIEW {_VIEW_NAME}_materialized AS
    SELECT * FROM {_VIEW_NAME};
    """

    reverse_sql_materialized = f"DROP MATERIALIZED VIEW IF EXISTS {_VIEW_NAME}_materialized;"

    operations = [
        migrations.RunSQL(
            sql=sql,
            reverse_sql=reverse_sql
        ),
        migrations.RunSQL(
            sql=sql_materialized,
            reverse_sql=reverse_sql_materialized
        ),
    ]
