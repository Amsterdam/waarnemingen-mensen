VIEW_STRINGS = {
    # NOTE: the regex in thes queries cause a DeprecationWarning: invalid escape sequence
    # For this reason we use a rawstring for these queries
    'cmsa_15min_view_v4': r"""CREATE VIEW cmsa_15min_view_v4 AS
    WITH rawdata AS (
        WITH v2_observatie_snelheid AS (
            WITH v2_observatie_persoon AS (
                WITH 
                    v2_observatie_persoon_3d AS (
                        SELECT 
                            observation_id,
                            speed,
                            (array_replace(regexp_matches(geom::text, '([0-9.]*)\)'::text), ''::text, '0'::text))[1]::numeric
                                - (array_replace(regexp_matches(geom::text, '\(([0-9.-]*)'::text), ''::text, '0'::text))[1]::numeric AS tijd
                        FROM telcameras_v2_personaggregate
                        WHERE speed IS NOT NULL
                        AND geom IS NOT NULL
                        AND geom::text <> ''::text
                    ), 
                    v2_observatie_persoon_2d AS (
                        SELECT
                            observation_id,
                            speed,
                            1 AS tijd
                        FROM telcameras_v2_personaggregate
                        WHERE speed IS NOT NULL
                        AND (geom IS NULL OR geom::text = ''::text)
                    )
                    SELECT 
                        v2_observatie_persoon_3d.observation_id,
                        v2_observatie_persoon_3d.speed,
                        v2_observatie_persoon_3d.tijd
                    FROM v2_observatie_persoon_3d UNION ALL SELECT 
                        v2_observatie_persoon_2d.observation_id,
                        v2_observatie_persoon_2d.speed,
                        v2_observatie_persoon_2d.tijd
                    FROM v2_observatie_persoon_2d
            )
            SELECT
                v2_observatie_persoon.observation_id,
                CASE
                    WHEN sum(v2_observatie_persoon.tijd) IS NOT NULL 
                    AND sum(v2_observatie_persoon.tijd) <> 0::numeric 
                    THEN 
                        round((sum(v2_observatie_persoon.speed * v2_observatie_persoon.tijd::double precision) / sum(v2_observatie_persoon.tijd)::double precision)::numeric, 2)
                    ELSE NULL::numeric
                END AS speed_avg
            FROM v2_observatie_persoon
            GROUP BY v2_observatie_persoon.observation_id
        ), 
        v2_countaggregate_zone_count AS (
            SELECT
                observation_id,
                max(azimuth) AS azimuth,
                max(count_in) AS count_in,
                max(count_out) AS count_out,
                max(area) AS area,
                max(count) AS count
            FROM telcameras_v2_countaggregate
            GROUP BY observation_id
        ),
        v1_data_uniek AS (
            SELECT max(id::text) AS idt
            FROM peoplemeasurement_peoplemeasurement
            GROUP BY sensor, "timestamp"
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
                v1_data_sel.sensor||'_v1' AS sensor,
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
                    WHEN c.area IS NOT NULL AND c.area <> 0::double precision AND c.count IS NOT NULL AND c.count > 0 THEN c.count::double precision / c.area
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
        FROM v1_data UNION ALL SELECT
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
            aggregatedbyquarter.sensor, (date_part('dow'::text, aggregatedbyquarter.timestamp_rounded)),
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
    LEFT JOIN percentiles p ON aq.sensor::text = p.sensor::TEXT
        AND date_part('dow'::text, aq.timestamp_rounded) = p.dayofweek::double PRECISION
        AND aq.timestamp_rounded::time without time zone = p.castedtimestamp
    ORDER BY
        aq.sensor,
        aq.timestamp_rounded
    ;
    """,

    'cmsa_15min_view_v5': r"""CREATE VIEW cmsa_15min_view_v5 AS
    WITH rawdata AS (
        WITH v2_feed_start_date as (select sensor, min(timestamp_start) as start_of_feed from telcameras_v2_observation group by sensor),
            v2_observatie_snelheid AS (
            WITH v2_observatie_persoon AS (
                WITH 
                    v2_observatie_persoon_3d AS (
                        SELECT 
                            observation_id,
                            speed,
                            (array_replace(regexp_matches(geom::text, '([0-9.]*)\)'::text), ''::text, '0'::text))[1]::numeric
                                - (array_replace(regexp_matches(geom::text, '\(([0-9.-]*)'::text), ''::text, '0'::text))[1]::numeric AS tijd
                        FROM telcameras_v2_personaggregate
                        WHERE speed IS NOT NULL
                        AND geom IS NOT NULL
                        AND geom::text <> ''::text
                    ), 
                    v2_observatie_persoon_2d AS (
                        SELECT
                            observation_id,
                            speed,
                            1 AS tijd
                        FROM telcameras_v2_personaggregate
                        WHERE speed IS NOT NULL
                        AND (geom IS NULL OR geom::text = ''::text)
                    )
                    SELECT 
                        v2_observatie_persoon_3d.observation_id,
                        v2_observatie_persoon_3d.speed,
                        v2_observatie_persoon_3d.tijd
                    FROM v2_observatie_persoon_3d UNION ALL SELECT 
                        v2_observatie_persoon_2d.observation_id,
                        v2_observatie_persoon_2d.speed,
                        v2_observatie_persoon_2d.tijd
                    FROM v2_observatie_persoon_2d
            )
            SELECT
                v2_observatie_persoon.observation_id,
                CASE
                    WHEN sum(v2_observatie_persoon.tijd) IS NOT NULL 
                    AND sum(v2_observatie_persoon.tijd) <> 0::numeric 
                    THEN 
                        round((sum(v2_observatie_persoon.speed * v2_observatie_persoon.tijd::double precision) / sum(v2_observatie_persoon.tijd)::double precision)::numeric, 2)
                    ELSE NULL::numeric
                END AS speed_avg
            FROM v2_observatie_persoon
            GROUP BY v2_observatie_persoon.observation_id
        ), 
        v2_countaggregate_zone_count AS (
            SELECT
                observation_id,
                max(azimuth) AS azimuth,
                max(count_in) AS count_in,
                max(count_out) AS count_out,
                max(area) AS area,
                max(count) AS count
            FROM telcameras_v2_countaggregate
            GROUP BY observation_id
        ),
        v1_data_uniek AS (
            SELECT max(id::text) AS idt
            FROM peoplemeasurement_peoplemeasurement
            GROUP BY sensor, "timestamp"
        ),
        v1_data_sel AS (
            SELECT
                dp.sensor,
                dp."timestamp",
                dp.details
            FROM peoplemeasurement_peoplemeasurement dp
            JOIN v1_data_uniek csdu ON dp.id::text = csdu.idt
            LEFT JOIN v2_feed_start_date v2dfsd ON v2dfsd.sensor=dp.sensor
            WHERE dp.timestamp<v2dfsd.start_of_feed
        ),
        v1_data AS (
            SELECT
                v1_data_sel.sensor||'_v1' AS sensor,
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
                    WHEN c.area IS NOT NULL AND c.area <> 0::double precision AND c.count IS NOT NULL AND c.count > 0 THEN c.count::double precision / c.area
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
        FROM v1_data UNION ALL SELECT
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
    LEFT JOIN percentiles p ON aq.sensor::text = p.sensor::TEXT
        AND date_part('dow'::text, aq.timestamp_rounded) = p.dayofweek::double PRECISION
        AND aq.timestamp_rounded::time without time zone = p.castedtimestamp
    ORDER BY
        aq.sensor,
        aq.timestamp_rounded
    ;
    """,

    'cmsa_15min_view_v6': r"""CREATE VIEW cmsa_15min_view_v6 AS
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
    """,

    'cmsa_15min_view_v6_realtime': r"""CREATE VIEW cmsa_15min_view_v6_realtime AS
    WITH v2_feed_start_date AS (
        SELECT o.sensor,
        min(o.timestamp_start) AS start_of_feed
        FROM telcameras_v2_observation o
        GROUP BY o.sensor
    ),
    mat_view_updated AS (
        SELECT m.sensor,
        max(m.timestamp_rounded) AS last_updated
        FROM cmsa_15min_view_v6_materialized m
        GROUP BY m.sensor
    ),
    v2_selectie AS (
        SELECT o.id
        FROM telcameras_v2_observation o
        LEFT JOIN mat_view_updated u ON o.sensor::text = u.sensor::text
        WHERE o.timestamp_start >= u.last_updated
        OR u.last_updated IS NULL
    ),
    rawdata AS (
        WITH v2_observatie_snelheid AS (
            WITH v2_observatie_persoon AS (
                WITH v2_observatie_persoon_3d AS (
                    SELECT
                        pa.observation_id,
                        pa.speed,
                        (array_replace(regexp_matches(pa.geom::text, '([0-9.]*)\)'::text), ''::text, '0'::text))[1]::numeric - (array_replace(regexp_matches(pa.geom::text, '\(([0-9.-]*)'::text), ''::text, '0'::text))[1]::numeric AS tijd
                    FROM telcameras_v2_personaggregate pa
                    JOIN v2_selectie s ON pa.observation_id = s.id
                    WHERE pa.speed IS NOT NULL
                    AND pa.geom IS NOT NULL
                    AND pa.geom::text <> ''::text
                ),
                v2_observatie_persoon_2d AS (
                    SELECT
                        pa.observation_id,
                        pa.speed,
                        1 AS tijd
                    FROM telcameras_v2_personaggregate pa
                    JOIN v2_selectie s ON pa.observation_id = s.id
                    WHERE pa.speed IS NOT NULL
                    AND (pa.geom IS NULL OR pa.geom::text = ''::text)
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
            SELECT
                v2_observatie_persoon.observation_id,
                CASE
                    WHEN sum(v2_observatie_persoon.tijd) IS NOT NULL AND sum(v2_observatie_persoon.tijd) <> 0::NUMERIC
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
            SELECT max(o.id::text) AS idt
            FROM peoplemeasurement_peoplemeasurement o
            LEFT JOIN mat_view_updated u ON o.sensor::text = u.sensor::text
            LEFT JOIN v2_feed_start_date v2dfsd ON v2dfsd.sensor::text = o.sensor::text
            WHERE o."timestamp" >= u.last_updated
            AND (o."timestamp" < v2dfsd.start_of_feed OR v2dfsd.start_of_feed IS NULL)
            GROUP BY o.sensor, o."timestamp"
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
            JOIN v2_selectie sel ON o.id = sel.id
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
    aggregatedbyquarter_new AS (
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
    aggregatedbyquarter AS (
        SELECT
            aggregatedbyquarter_new.sensor,
            aggregatedbyquarter_new.timestamp_rounded,
            aggregatedbyquarter_new.total_count,
            aggregatedbyquarter_new.count_down,
            aggregatedbyquarter_new.count_up,
            aggregatedbyquarter_new.density_avg,
            aggregatedbyquarter_new.speed_avg,
            aggregatedbyquarter_new.basedonxmessages
        FROM aggregatedbyquarter_new
        UNION
        SELECT
            cmsa_15min_view_v6_materialized.sensor,
            cmsa_15min_view_v6_materialized.timestamp_rounded,
            cmsa_15min_view_v6_materialized.total_count,
            cmsa_15min_view_v6_materialized.count_down,
            cmsa_15min_view_v6_materialized.count_up,
            cmsa_15min_view_v6_materialized.density_avg,
            cmsa_15min_view_v6_materialized.speed_avg,
            cmsa_15min_view_v6_materialized.basedonxmessages
        FROM cmsa_15min_view_v6_materialized
        JOIN mat_view_updated ON mat_view_updated.sensor::text = cmsa_15min_view_v6_materialized.sensor::text
        WHERE cmsa_15min_view_v6_materialized.timestamp_rounded < mat_view_updated.last_updated
    ),
    percentiles AS (
        SELECT
            cmsa_15min_view_v6_materialized.sensor,
            date_part('dow'::text, cmsa_15min_view_v6_materialized.timestamp_rounded) AS dayofweek,
            cmsa_15min_view_v6_materialized.timestamp_rounded::time without time zone AS castedtimestamp,
            avg(cmsa_15min_view_v6_materialized.total_count_p10) AS total_count_p10,
            avg(cmsa_15min_view_v6_materialized.total_count_p20) AS total_count_p20,
            avg(cmsa_15min_view_v6_materialized.total_count_p50) AS total_count_p50,
            avg(cmsa_15min_view_v6_materialized.total_count_p80) AS total_count_p80,
            avg(cmsa_15min_view_v6_materialized.total_count_p90) AS total_count_p90,
            avg(cmsa_15min_view_v6_materialized.count_down_p10) AS count_down_p10,
            avg(cmsa_15min_view_v6_materialized.count_down_p20) AS count_down_p20,
            avg(cmsa_15min_view_v6_materialized.count_down_p50) AS count_down_p50,
            avg(cmsa_15min_view_v6_materialized.count_down_p80) AS count_down_p80,
            avg(cmsa_15min_view_v6_materialized.count_down_p90) AS count_down_p90,
            avg(cmsa_15min_view_v6_materialized.count_up_p10) AS count_up_p10,
            avg(cmsa_15min_view_v6_materialized.count_up_p20) AS count_up_p20,
            avg(cmsa_15min_view_v6_materialized.count_up_p50) AS count_up_p50,
            avg(cmsa_15min_view_v6_materialized.count_up_p80) AS count_up_p80,
            avg(cmsa_15min_view_v6_materialized.count_up_p90) AS count_up_p90,
            avg(cmsa_15min_view_v6_materialized.density_avg_p20) AS density_avg_p20,
            avg(cmsa_15min_view_v6_materialized.density_avg_p50) AS density_avg_p50,
            avg(cmsa_15min_view_v6_materialized.density_avg_p80) AS density_avg_p80,
            avg(cmsa_15min_view_v6_materialized.speed_avg_p20) AS speed_avg_p20,
            avg(cmsa_15min_view_v6_materialized.speed_avg_p50) AS speed_avg_p50,
            avg(cmsa_15min_view_v6_materialized.speed_avg_p80) AS speed_avg_p80
        FROM cmsa_15min_view_v6_materialized
        WHERE cmsa_15min_view_v6_materialized.timestamp_rounded >= (( SELECT now() - '1 year'::interval))
        GROUP BY
            cmsa_15min_view_v6_materialized.sensor,
            (date_part('dow'::text, cmsa_15min_view_v6_materialized.timestamp_rounded)),
            (cmsa_15min_view_v6_materialized.timestamp_rounded::time without time zone)
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
    LEFT JOIN percentiles p ON aq.sensor::text = p.sensor::text AND date_part('dow'::text, aq.timestamp_rounded) = p.dayofweek AND aq.timestamp_rounded::time without time zone = p.castedtimestamp
    ORDER BY aq.sensor, aq.timestamp_rounded
    ;
    """
}


def get_view_strings(view_name):
    reverse_sql = f"DROP VIEW IF EXISTS {view_name};"

    sql_materialized = f"""
        CREATE MATERIALIZED VIEW {view_name}_materialized AS
        SELECT * FROM {view_name};
        """

    reverse_sql_materialized = f"DROP MATERIALIZED VIEW IF EXISTS {view_name}_materialized;"

    return {
        'sql': VIEW_STRINGS[view_name],
        'reverse_sql': reverse_sql,
        'sql_materialized': sql_materialized,
        'reverse_sql_materialized': reverse_sql_materialized,
    }
