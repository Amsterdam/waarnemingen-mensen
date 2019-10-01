from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('peoplemeasurement', '0003_auto_20190808_1432'),
    ]

    _VIEW_NAME = "cmsa_15min"

    sql = f"""
CREATE VIEW {_VIEW_NAME} AS
SELECT
    timestamp_rounded as timestamp,
    sensor,
    based_on_x_messages,
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
    (SELECT
        *,
        to_timestamp(EXTRACT(epoch from timestamp)::int / 60 / 15 * 15 * 60) as timestamp_rounded,  -- round down for every 15 minutes
        COUNT(*) OVER (PARTITION BY EXTRACT(epoch from "timestamp")::int / 60 / 15, sensor) AS based_on_x_messages
    FROM peoplemeasurement_peoplemeasurement
    ) s,
    jsonb_array_elements(details) detail_elems
GROUP BY
    timestamp_rounded,
    sensor,
    based_on_x_messages
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