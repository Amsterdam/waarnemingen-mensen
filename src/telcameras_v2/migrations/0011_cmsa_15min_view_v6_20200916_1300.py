from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('telcameras_v2', '0010_auto_20200911_1519'),
    ]

    # VIEW DESCRIPTION: This view only uses data from the telcameras_v2 from the time we actually have data, and
    # then disregards data from peoplemeasurement (v1)
    _VIEW_NAME = "cmsa_15min_view_v6"

    # NOTE: the regex in this query causes a DeprecationWarning: invalid escape sequence
    # For this reason we use a rawstring for that part of the query
    sql = f"CREATE VIEW {_VIEW_NAME} AS" + r"""
WITH rawdata AS (
    WITH v2_feed_start_date AS (
        SELECT o.sensor,
        min(o.timestamp_start) AS start_of_feed
        FROM telcameras_v2_observation o
        GROUP BY o.sensor
    ),
    v2_observatie_snelheid AS (
        WITH v2_observatie_persoon AS (
            WITH v2_observatie_persoon_3d AS (
                SELECT
                    p.observation_id,
                    p.speed,
                    (array_replace(regexp_matches(p.geom::text, '([0-9.]*)\)'::text), ''::text, '0'::text))[1]::numeric - (array_replace(regexp_matches(p.geom::text, '\(([0-9.-]*)'::text), ''::text, '0'::text))[1]::numeric AS tijd
                FROM telcameras_v2_personaggregate p
                WHERE p.speed IS NOT NULL
                AND p.geom IS NOT NULL
                AND p.geom::text <> ''::text
            ),
            v2_observatie_persoon_2d AS (
                SELECT
                    p.observation_id,
                    p.speed,
                    1 AS tijd
                FROM telcameras_v2_personaggregate p
                WHERE p.speed IS NOT NULL
                AND (p.geom IS NULL OR p.geom::text = ''::text)
            )
            SELECT
                v2_observatie_persoon_3d.observation_id,
                v2_observatie_persoon_3d.speed,
                v2_observatie_persoon_3d.tijd
            FROM v2_observatie_persoon_3d
            UNION ALL
            SELECT
                v2_observatie_persoon_2d.observation_id,
                v2_observatie_persoon_2d.speed,
                v2_observatie_persoon_2d.tijd
            FROM v2_observatie_persoon_2d
        )
        SELECT v2_observatie_persoon.observation_id,
        CASE
            WHEN sum(v2_observatie_persoon.tijd) IS NOT NULL AND sum(v2_observatie_persoon.tijd) <> 0::numeric 
            THEN round((sum(v2_observatie_persoon.speed * v2_observatie_persoon.tijd::double precision) / sum(v2_observatie_persoon.tijd)::double precision)::numeric, 2)
            ELSE NULL::numeric
        END AS speed_avg
        FROM v2_observatie_persoon
        GROUP BY v2_observatie_persoon.observation_id
    ),
    v2_countaggregate_zone_count AS (
        SELECT 
            c.observation_id,
            max(c.azimuth) AS azimuth,
            max(c.count_in) AS count_in,
            max(c.count_out) AS count_out,
            max(c.area) AS area,
            max(c.count) AS count
        FROM telcameras_v2_countaggregate c
        GROUP BY c.observation_id
    ),
    v1_data_uniek AS (
        SELECT max(a.id::text) AS idt
        FROM peoplemeasurement_peoplemeasurement a
        LEFT JOIN v2_feed_start_date fsd ON fsd.sensor::text = a.sensor::text
        WHERE a."timestamp" < fsd.start_of_feed
        OR fsd.start_of_feed IS NULL
        GROUP BY a.sensor, a."timestamp"
    ),
    v1_data_sel AS (
        SELECT 
            dp.sensor,
            dp."timestamp",
            dp.details
        FROM peoplemeasurement_peoplemeasurement dp
        JOIN v1_data_uniek csdu ON dp.id::text = csdu.idt
    ),
    v1_data AS (
        SELECT
            v1_data_sel.sensor,
            v1_data_sel."timestamp",
            COALESCE(sum((detail_elems.value ->> 'count'::text)::integer) FILTER (WHERE (detail_elems.value ->> 'direction'::text) = 'down'::text), 0::bigint) + COALESCE(sum((detail_elems.value ->> 'count'::text)::integer) FILTER (WHERE (detail_elems.value ->> 'direction'::text) = 'up'::text), 0::bigint) AS total_count,
            COALESCE(sum((detail_elems.value ->> 'count'::text)::integer) FILTER (WHERE (detail_elems.value ->> 'direction'::text) = 'down'::text), 0::bigint) AS count_down,
            COALESCE(sum((detail_elems.value ->> 'count'::text)::integer) FILTER (WHERE (detail_elems.value ->> 'direction'::text) = 'up'::text), 0::bigint) AS count_up,
            avg((detail_elems.value ->> 'count'::text)::numeric) FILTER (WHERE (detail_elems.value ->> 'direction'::text) = 'density'::text) AS density_avg,
            avg((detail_elems.value ->> 'count'::text)::numeric) FILTER (WHERE (detail_elems.value ->> 'direction'::text) = 'speed'::text) AS speed_avg
        FROM v1_data_sel,
        LATERAL jsonb_array_elements(v1_data_sel.details) detail_elems(value)
        GROUP BY v1_data_sel.sensor, v1_data_sel."timestamp"
        ORDER BY v1_data_sel.sensor, v1_data_sel."timestamp"
    ),
    v2_data AS (
        SELECT
            o.sensor,
            o.timestamp_start AS "timestamp",
            COALESCE((c.count_in + c.count_out)::integer, 0) AS total_count,
            COALESCE(c.count_in::integer, 0) AS count_up,
            COALESCE(c.count_out::integer, 0) AS count_down,
            CASE
                WHEN c.area IS NOT NULL AND c.area <> 0::double precision AND c.count IS NOT NULL AND c.count > 0
                THEN c.count::double precision / c.area
                ELSE NULL::double precision
            END AS density_avg,
            s.speed_avg
        FROM telcameras_v2_observation o
        LEFT JOIN v2_observatie_snelheid s ON o.id = s.observation_id
        LEFT JOIN v2_countaggregate_zone_count c ON o.id = c.observation_id
    )
    SELECT
        v1_data.sensor,
        v1_data."timestamp",
        v1_data.total_count,
        v1_data.count_down,
        v1_data.count_up,
        v1_data.density_avg,
        v1_data.speed_avg
    FROM v1_data
    UNION ALL
    SELECT
        v2_data.sensor,
        v2_data."timestamp",
        v2_data.total_count,
        v2_data.count_up,
        v2_data.count_down,
        v2_data.density_avg,
        v2_data.speed_avg
    FROM v2_data
),
aggregatedbyquarter AS (
    SELECT
        rawdata.sensor,
        date_trunc('hour'::text, rawdata."timestamp") + (date_part('minute'::text, rawdata."timestamp")::integer / 15)::double precision * '00:15:00'::interval AS timestamp_rounded,
        round(avg(rawdata.total_count) * 15::numeric, 0) AS total_count,
        round(avg(rawdata.count_down) * 15::numeric, 0) AS count_down,
        round(avg(rawdata.count_up) * 15::numeric, 0) AS count_up,
        avg(rawdata.density_avg) AS density_avg,
        avg(rawdata.speed_avg) AS speed_avg,
        count(*) AS basedonxmessages
    FROM rawdata
    GROUP BY rawdata.sensor, (date_trunc('hour'::text, rawdata."timestamp") + (date_part('minute'::text, rawdata."timestamp")::integer / 15)::double precision * '00:15:00'::interval)
    ORDER BY rawdata.sensor, (date_trunc('hour'::text, rawdata."timestamp") + (date_part('minute'::text, rawdata."timestamp")::integer / 15)::double precision * '00:15:00'::interval)
),
percentiles AS (
    SELECT
        aggregatedbyquarter.sensor,
        date_part('dow'::text, aggregatedbyquarter.timestamp_rounded)::integer AS dayofweek,
        aggregatedbyquarter.timestamp_rounded::time without time zone AS castedtimestamp,
        percentile_disc(0.1::double precision) WITHIN GROUP (ORDER BY aggregatedbyquarter.total_count) AS total_count_p10,
        percentile_disc(0.2::double precision) WITHIN GROUP (ORDER BY aggregatedbyquarter.total_count) AS total_count_p20,
        percentile_disc(0.5::double precision) WITHIN GROUP (ORDER BY aggregatedbyquarter.total_count) AS total_count_p50,
        percentile_disc(0.8::double precision) WITHIN GROUP (ORDER BY aggregatedbyquarter.total_count) AS total_count_p80,
        percentile_disc(0.9::double precision) WITHIN GROUP (ORDER BY aggregatedbyquarter.total_count) AS total_count_p90,
        percentile_disc(0.1::double precision) WITHIN GROUP (ORDER BY aggregatedbyquarter.count_down) AS count_down_p10,
        percentile_disc(0.2::double precision) WITHIN GROUP (ORDER BY aggregatedbyquarter.count_down) AS count_down_p20,
        percentile_disc(0.5::double precision) WITHIN GROUP (ORDER BY aggregatedbyquarter.count_down) AS count_down_p50,
        percentile_disc(0.8::double precision) WITHIN GROUP (ORDER BY aggregatedbyquarter.count_down) AS count_down_p80,
        percentile_disc(0.9::double precision) WITHIN GROUP (ORDER BY aggregatedbyquarter.count_down) AS count_down_p90,
        percentile_disc(0.1::double precision) WITHIN GROUP (ORDER BY aggregatedbyquarter.count_up) AS count_up_p10,
        percentile_disc(0.2::double precision) WITHIN GROUP (ORDER BY aggregatedbyquarter.count_up) AS count_up_p20,
        percentile_disc(0.5::double precision) WITHIN GROUP (ORDER BY aggregatedbyquarter.count_up) AS count_up_p50,
        percentile_disc(0.8::double precision) WITHIN GROUP (ORDER BY aggregatedbyquarter.count_up) AS count_up_p80,
        percentile_disc(0.9::double precision) WITHIN GROUP (ORDER BY aggregatedbyquarter.count_up) AS count_up_p90,
        percentile_disc(0.2::double precision) WITHIN GROUP (ORDER BY aggregatedbyquarter.density_avg) AS density_avg_p20,
        percentile_disc(0.5::double precision) WITHIN GROUP (ORDER BY aggregatedbyquarter.density_avg) AS density_avg_p50,
        percentile_disc(0.8::double precision) WITHIN GROUP (ORDER BY aggregatedbyquarter.density_avg) AS density_avg_p80,
        percentile_disc(0.2::double precision) WITHIN GROUP (ORDER BY aggregatedbyquarter.speed_avg) AS speed_avg_p20,
        percentile_disc(0.5::double precision) WITHIN GROUP (ORDER BY aggregatedbyquarter.speed_avg) AS speed_avg_p50,
        percentile_disc(0.8::double precision) WITHIN GROUP (ORDER BY aggregatedbyquarter.speed_avg) AS speed_avg_p80
    FROM aggregatedbyquarter
    WHERE aggregatedbyquarter.timestamp_rounded >= (( SELECT now() - '1 year'::interval))
    GROUP BY
        aggregatedbyquarter.sensor,
        (date_part('dow'::text, aggregatedbyquarter.timestamp_rounded)),
        (aggregatedbyquarter.timestamp_rounded::time without time zone)
)
SELECT
    aq.sensor,
    aq.timestamp_rounded,
    aq.total_count,
    aq.count_down,
    aq.count_up,
    aq.density_avg,
    aq.speed_avg,
    aq.basedonxmessages,
    p.total_count_p10,
    p.total_count_p20,
    p.total_count_p50,
    p.total_count_p80,
    p.total_count_p90,
    p.count_down_p10,
    p.count_down_p20,
    p.count_down_p50,
    p.count_down_p80,
    p.count_down_p90,
    p.count_up_p10,
    p.count_up_p20,
    p.count_up_p50,
    p.count_up_p80,
    p.count_up_p90,
    p.density_avg_p20,
    p.density_avg_p50,
    p.density_avg_p80,
    p.speed_avg_p20,
    p.speed_avg_p50,
    p.speed_avg_p80
FROM aggregatedbyquarter aq
LEFT JOIN percentiles p ON aq.sensor::text = p.sensor::TEXT AND date_part('dow'::text, aq.timestamp_rounded) = p.dayofweek::double precision AND aq.timestamp_rounded::time without time zone = p.castedtimestamp
ORDER BY aq.sensor, aq.timestamp_rounded
;
"""

    reverse_sql = f"DROP VIEW IF EXISTS {_VIEW_NAME};"

    sql_materialized = f"""
    CREATE MATERIALIZED VIEW cmsa_15min_view_v6_materialized AS SELECT * FROM cmsa_15min_view_v6;
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