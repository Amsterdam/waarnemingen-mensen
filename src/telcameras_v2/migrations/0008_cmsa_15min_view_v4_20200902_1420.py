from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('telcameras_v2', '0007_auto_20200803_1902'),
    ]

    _VIEW_NAME = "cmsa_15min_view_v4"

    # NOTE: the regex in this query causes a DeprecationWarning: invalid escape sequence
    # For this reason we use a rawstring for that part of the query
    sql = f"CREATE VIEW {_VIEW_NAME} AS" + r"""
with rawdata as(
    with observatie_snelheid as(
        with observatie_persoon as (
            with observatie_persoon_3d as (     --bepaal tijdsduur op basis van de geom data
                select 
                    observation_id,             --dit is alleen bij 3D camera's gevuld
                    speed,                      --negeer person records met onbepaalde speed null
                    (array_replace(regexp_matches(geom,'([0-9.]*)\)'),'','0'))[1]::numeric-
                        (array_replace(regexp_matches(geom,'\(([0-9.-]*)'),'','0'))[1]::numeric as tijd
                from telcameras_v2_personaggregate
                where speed is not null
                and geom is not null
                and geom != ''
            ),
             observatie_persoon_2d as (  --voor records uit 2D camera's die geen geom data hebben
                select
                    observation_id,
                    speed, 
                    1 as tijd            -- bereken de gemiddelde snelheid op basis van een ongewogen gemiddelde
                from telcameras_v2_personaggregate 
                where speed is not null 
                and (geom is null or geom = '')
            )
            select * from observatie_persoon_3d union all select * from observatie_persoon_2d 
        )
        select 
            observation_id, 
            CASE
                when sum(tijd) is not null and sum(tijd) != 0 then round((sum(speed*tijd)/sum(tijd))::numeric,2)
                ELSE null
            END as speed_avg
        from observatie_persoon 
        group by observation_id
    ),
    countaggregate_zone_count as (
        select 
            observation_id,
            max(azimuth) as azimuth, 
            max(count_in) as count_in, 
            max(count_out) as count_out,
            max(area) as area, 
            max(count) as count 
        from telcameras_v2_countaggregate 
        group by observation_id)
    select 
        o.sensor,
        o.timestamp_start as timestamp,
        COALESCE((count_in+count_out),0) as total_count, 
        COALESCE(count_in,0) as count_up, COALESCE(count_out,0) as count_down, 
        CASE
            when area is not null and area != 0 and count is not null and count>0 then count/area
            ELSE null
        END as density_avg, 
    s.speed_avg   
    from telcameras_v2_observation o
    LEFT JOIN observatie_snelheid s on o.id=s.observation_id
    LEFT JOIN countaggregate_zone_count c on o.id=c.observation_id
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
    group by sensor, timestamp_rounded
    order by sensor, timestamp_rounded),
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
        timestamp_rounded::time)
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
