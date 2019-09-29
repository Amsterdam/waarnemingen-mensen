from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('peoplemeasurement', '0003_auto_20190808_1432'),
    ]

    _VIEW_NAME = "cmsa_15min"

    sql = f"""
CREATE VIEW {_VIEW_NAME} AS
WITH tmp AS (
    SELECT
        *,
        CASE -- round by every 15 minutes
            WHEN extract('minute' from timestamp) < 15 THEN date_trunc('hour', timestamp)
            WHEN extract('minute' from timestamp) < 30 THEN date_trunc('hour', timestamp) + '15 minutes'
            WHEN extract('minute' from timestamp) < 45 THEN date_trunc('hour', timestamp) + '30 minutes'
            ELSE date_trunc('hour', timestamp) + '45 minutes'
        END AS timestamp_rounded
    FROM peoplemeasurement_peoplemeasurement

)
SELECT
    timestamp_rounded as timestamp,
    sensor,
    (SELECT count(*) from tmp tmp2 where tmp2.timestamp_rounded=tmp.timestamp_rounded and tmp2.sensor=tmp.sensor) as based_on_x_messages,
    sum((detail_elems ->> 'count')::numeric) FILTER (WHERE detail_elems ->> 'direction' = 'up') AS up_sum,
    sum((detail_elems ->> 'count')::numeric) FILTER (WHERE detail_elems ->> 'direction' = 'down') AS down_sum,
    percentile_disc(0.1) WITHIN GROUP (ORDER BY (detail_elems ->> 'count')::numeric) FILTER (WHERE detail_elems ->> 'direction' = 'density') AS density_p10,
    percentile_disc(0.2) WITHIN GROUP (ORDER BY (detail_elems ->> 'count')::numeric) FILTER (WHERE detail_elems ->> 'direction' = 'density') AS density_p20,
    percentile_disc(0.5) WITHIN GROUP (ORDER BY (detail_elems ->> 'count')::numeric) FILTER (WHERE detail_elems ->> 'direction' = 'density') AS density_p50,
    percentile_disc(0.8) WITHIN GROUP (ORDER BY (detail_elems ->> 'count')::numeric) FILTER (WHERE detail_elems ->> 'direction' = 'density') AS density_p80,
    percentile_disc(0.9) WITHIN GROUP (ORDER BY (detail_elems ->> 'count')::numeric) FILTER (WHERE detail_elems ->> 'direction' = 'density') AS density_p90,
    avg((detail_elems ->> 'count')::numeric) FILTER (WHERE detail_elems ->> 'direction' = 'density') AS density_avg,
    percentile_disc(0.1) WITHIN GROUP (ORDER BY (detail_elems ->> 'count')::numeric) FILTER (WHERE detail_elems ->> 'direction' = 'speed') AS speed_p10,
    percentile_disc(0.2) WITHIN GROUP (ORDER BY (detail_elems ->> 'count')::numeric) FILTER (WHERE detail_elems ->> 'direction' = 'speed') AS speed_p20,
    percentile_disc(0.5) WITHIN GROUP (ORDER BY (detail_elems ->> 'count')::numeric) FILTER (WHERE detail_elems ->> 'direction' = 'speed') AS speed_p50,
    percentile_disc(0.8) WITHIN GROUP (ORDER BY (detail_elems ->> 'count')::numeric) FILTER (WHERE detail_elems ->> 'direction' = 'speed') AS speed_p80,
    percentile_disc(0.9) WITHIN GROUP (ORDER BY (detail_elems ->> 'count')::numeric) FILTER (WHERE detail_elems ->> 'direction' = 'speed') AS speed_p90,
    avg((detail_elems ->> 'count')::numeric) FILTER (WHERE detail_elems ->> 'direction' = 'speed') AS speed_avg
FROM
    tmp,
    jsonb_array_elements(details) detail_elems
GROUP BY
    timestamp_rounded,
    sensor
ORDER BY
    timestamp_rounded ASC,
    sensor ASC
;"""

    reverse_sql = f"drop view if exists {_VIEW_NAME};"

    operations = [
        migrations.RunSQL(
            sql=sql,
            reverse_sql=reverse_sql
        ),
    ]