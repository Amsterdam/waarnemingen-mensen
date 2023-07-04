from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("peoplemeasurement", "0005_cmsa_15min_materialized_view_20200304_1633"),
    ]

    _VIEW_NAME = "cmsa_15min_view_v2"

    sql = f"""
CREATE VIEW {_VIEW_NAME} AS
with RawData as(
    select
        sensor,
        timestamp,
        coalesce(sum((detail_elems ->> 'count')::integer) filter (where detail_elems ->> 'direction' = 'down'), 0) + coalesce(sum((detail_elems ->> 'count')::integer) filter (where detail_elems ->> 'direction' = 'up'), 0) as total_count,
        coalesce(sum((detail_elems ->> 'count')::integer) filter (where detail_elems ->> 'direction' = 'down'), 0) as count_down,
        coalesce(sum((detail_elems ->> 'count')::integer) filter (where detail_elems ->> 'direction' = 'up'), 0) as count_up,
        avg((detail_elems ->> 'count')::numeric) filter (where detail_elems ->> 'direction' = 'density') as density_avg,
        avg((detail_elems ->> 'count')::numeric) filter (where detail_elems ->> 'direction' = 'speed') as speed_avg
    from peoplemeasurement_peoplemeasurement, jsonb_array_elements(details) detail_elems
    group by sensor, timestamp
    order by sensor, timestamp
),
AggregatedByQuarter as(
    select
        sensor,
        date_trunc('hour', timestamp) + date_part('minute', timestamp)::int / 15 * interval '15 min' as timestamp_rounded,
        round((avg(total_count) * 15), 0) as total_count,
        round((avg(count_down) * 15), 0) as count_down,
        round((avg(count_up) * 15), 0) as count_up,
        avg(density_avg) as density_avg,
        avg(speed_avg) as speed_avg,
        count(*) as BasedOnXMessages
    from RawData
    where total_count != 0
    group by sensor, timestamp_rounded
    order by sensor, timestamp_rounded
),
Percentiles as(
    select
        sensor,
        extract(dow from timestamp_rounded)::int as dayofweek,
        timestamp_rounded::time as CastedTimeStamp,
        percentile_disc(0.1) within group (order by total_count) as total_count_p10,
        percentile_disc(0.2) within group (order by total_count) as total_count_p20,
        percentile_disc(0.5) within group (order by total_count) as total_count_p50,
        percentile_disc(0.8) within group (order by total_count) as total_count_p80,
        percentile_disc(0.9) within group (order by total_count) as total_count_p90,
        percentile_disc(0.1) within group (order by count_down) as count_down_p10,
        percentile_disc(0.2) within group (order by count_down) as count_down_p20,
        percentile_disc(0.5) within group (order by count_down) as count_down_p50,
        percentile_disc(0.8) within group (order by count_down) as count_down_p80,
        percentile_disc(0.9) within group (order by count_down) as count_down_p90,
        percentile_disc(0.1) within group (order by count_up) as count_up_p10,
        percentile_disc(0.2) within group (order by count_up) as count_up_p20,
        percentile_disc(0.5) within group (order by count_up) as count_up_p50,
        percentile_disc(0.8) within group (order by count_up) as count_up_p80,
        percentile_disc(0.9) within group (order by count_up) as count_up_p90,
        percentile_disc(0.2) within group (order by density_avg) as density_avg_p20,
        percentile_disc(0.5) within group (order by density_avg) as density_avg_p50,
        percentile_disc(0.8) within group (order by density_avg) as density_avg_p80,
        percentile_disc(0.2) within group (order by speed_avg) as speed_avg_p20,
        percentile_disc(0.5) within group (order by speed_avg) as speed_avg_p50,
        percentile_disc(0.8) within group (order by speed_avg) as speed_avg_p80
    from AggregatedByQuarter
    where timestamp_rounded >= (select now() - interval '1 year')
    group by
        sensor,
        extract(dow from timestamp_rounded),
        timestamp_rounded::time
)
select
    aq.*,
    total_count_p10,
    total_count_p20,
    total_count_p50,
    total_count_p80,
    total_count_p90,
    count_down_p10,
    count_down_p20,
    count_down_p50,
    count_down_p80,
    count_down_p90,
    count_up_p10,
    count_up_p20,
    count_up_p50,
    count_up_p80,
    count_up_p90,
    density_avg_p20,
    density_avg_p50,
    density_avg_p80,
    speed_avg_p20,
    speed_avg_p50,
    speed_avg_p80
from AggregatedByQuarter aq
left join percentiles p on
    aq.sensor = p.sensor
    and extract(dow from aq.timestamp_rounded) = p.DayOfWeek
    and aq.timestamp_rounded::time = p.CastedTimeStamp
order by
    sensor,
    timestamp_rounded
;
"""

    reverse_sql = f"DROP VIEW IF EXISTS {_VIEW_NAME};"

    sql_materialized = f"""
    CREATE MATERIALIZED VIEW {_VIEW_NAME}_materialized AS
    SELECT * FROM {_VIEW_NAME};
    """

    reverse_sql_materialized = (
        f"DROP MATERIALIZED VIEW IF EXISTS {_VIEW_NAME}_materialized;"
    )

    operations = [
        migrations.RunSQL(sql=sql, reverse_sql=reverse_sql),
        migrations.RunSQL(sql=sql_materialized, reverse_sql=reverse_sql_materialized),
    ]
