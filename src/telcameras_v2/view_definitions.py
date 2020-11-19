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
    """,

    'cmsa_15min_view_v7': r"""CREATE VIEW cmsa_15min_view_v7 AS
    WITH v2_feed_start_date AS
            (SELECT o.sensor,
            min(o.timestamp_start) AS start_of_feed
            FROM telcameras_v2_observation o
            GROUP BY o.sensor)
    ,v1_data_uniek AS (
            SELECT a.sensor, a.timestamp, max(a.id::text) AS idt FROM peoplemeasurement_peoplemeasurement a
            LEFT JOIN v2_feed_start_date fsd ON fsd.sensor::text = a.sensor::text
            WHERE a."timestamp" < fsd.start_of_feed OR fsd.start_of_feed IS NULL
            GROUP BY a.sensor, a."timestamp")
    , v1_data_sel AS (
            SELECT dp.sensor, dp.timestamp, 
            date_trunc('hour'::text, dp.timestamp) + (date_part('minute'::text, dp.timestamp)::integer / 15)::double precision * '00:15:00'::interval AS timestamp_rounded,
            1::integer as aantal,
            dp.details
            FROM peoplemeasurement_peoplemeasurement dp
            JOIN v1_data_uniek csdu ON dp.id::text = csdu.idt and dp.timestamp=csdu.timestamp)
    ,v1_data AS     (
            SELECT ds.sensor,
            ds.timestamp_rounded,
            count(distinct(ds.timestamp) ) as basedonxmessages,
            COALESCE(sum((detail_elems.value ->> 'count'::text)::integer) FILTER (WHERE (detail_elems.value ->> 'direction'::text) = 'down'::text), 0::bigint) + COALESCE(sum((detail_elems.value ->> 'count'::text)::integer) FILTER (WHERE (detail_elems.value ->> 'direction'::text) = 'up'::text), 0::bigint) AS total_count,
            COALESCE(sum((detail_elems.value ->> 'count'::text)::integer) FILTER (WHERE (detail_elems.value ->> 'direction'::text) = 'down'::text), 0::bigint) AS count_down,
             COALESCE(sum((detail_elems.value ->> 'count'::text)::integer) FILTER (WHERE (detail_elems.value ->> 'direction'::text) = 'up'::text), 0::bigint) AS count_up,
            avg((detail_elems.value ->> 'count'::text)::numeric) FILTER (WHERE (detail_elems.value ->> 'direction'::text) = 'density'::text) AS density_avg,
            avg((detail_elems.value ->> 'count'::text)::numeric) FILTER (WHERE (detail_elems.value ->> 'direction'::text) = 'speed'::text) AS speed_avg
            FROM v1_data_sel ds,
            LATERAL jsonb_array_elements(ds.details) detail_elems(value)
            GROUP BY ds.sensor, ds.timestamp_rounded
            ORDER BY ds.sensor, ds.timestamp_rounded
            )
    ,v2_selectie AS    (SELECT o.id,o.sensor, o.timestamp_start,
            date_trunc('hour'::text, o.timestamp_start) + (date_part('minute'::text, o.timestamp_start)::integer / 15)::double precision * '00:15:00'::interval AS timestamp_rounded,
            1::integer as aantal
            FROM telcameras_v2_observation o
            WHERE id IN (SELECT id FROM(SELECT id,ROW_NUMBER() OVER(PARTITION BY sensor, timestamp_start ORDER BY sensor, timestamp_start, timestamp_message DESC )
            AS row_num FROM telcameras_v2_observation ) t WHERE t.row_num = 1)
            AND o.timestamp_start > now()- INTERVAL '1 YEAR')
    ,v2_sensor_15min_sel as    (select sensor, timestamp_rounded, sum(aantal) as basedonxmessages from v2_selectie group by sensor, timestamp_rounded order by sensor, timestamp_rounded)
    ,v2_observatie_snelheid AS     (
            with v2_observatie_persoon AS (
                    SELECT sel.sensor, sel.timestamp_rounded,
                    pa.speed,
                    string_to_array(substr(pa.geom,position('('IN pa.geom)+1, (position(')'IN pa.geom)- position('('IN pa.geom)-1)), ' ' ) as tijd_array
                    FROM telcameras_v2_personaggregate pa
                    JOIN v2_selectie sel ON pa.observation_id = sel.id AND pa.observation_timestamp_start=sel.timestamp_start
                    WHERE pa.speed IS NOT NULL AND pa.geom IS NOT NULL AND pa.geom::text <> ''::text
                UNION ALL
                    SELECT sel2.sensor, sel2.timestamp_rounded,
                    pa.speed,
                    ARRAY['1','2'] AS tijd_array
                    FROM telcameras_v2_personaggregate pa
                    JOIN v2_selectie sel2 ON pa.observation_id = sel2.id AND pa.observation_timestamp_start=sel2.timestamp_start
                    WHERE pa.speed IS NOT NULL AND (pa.geom IS NULL OR pa.geom::text = ''::text)  )
            SELECT p.sensor, p.timestamp_rounded,
            CASE WHEN sum(tijd_array[cardinality(tijd_array)]::numeric - tijd_array[1]::numeric)>0 THEN (sum(p.speed * (tijd_array[cardinality(tijd_array)]::numeric - tijd_array[1]::numeric)) / sum(tijd_array[cardinality(tijd_array)]::numeric - tijd_array[1]::numeric)) ELSE 0 END  AS speed_avg
            FROM v2_observatie_persoon p
            GROUP BY p.sensor, p.timestamp_rounded)
    ,v2_countaggregate_zone_count AS     (  
            SELECT sel.sensor, sel.timestamp_rounded, 
            max(c.azimuth) AS azimuth,
            sum(c.count_in) AS count_in,
            sum(c.count_out) AS count_out,
            sum(c.count_in+c.count_out) as total_count,
            avg(c.count) as area_count,
            max(c.area) AS area
            FROM telcameras_v2_countaggregate c
            JOIN v2_selectie sel ON c.observation_id = sel.id AND c.observation_timestamp_start=sel.timestamp_start
            GROUP BY sel.sensor, sel.timestamp_rounded )
    ,v2_data AS     (SELECT 
              sel3.sensor,
              sel3.timestamp_rounded,
                COALESCE(oc.total_count::integer, 0) AS total_count,
                COALESCE(oc.count_in::integer, 0) AS count_up,
                COALESCE(oc.count_out::integer, 0) AS count_down,
                   CASE
                       WHEN oc.area IS NOT NULL AND oc.area <> 0::double precision AND oc.area_count IS NOT NULL AND oc.area_count > 0 
                            THEN oc.area_count::double precision / oc.area
                      ELSE NULL::double precision
                   END AS density_avg,
                os.speed_avg, 
                sel3.basedonxmessages
               FROM v2_sensor_15min_sel sel3
                 LEFT JOIN v2_observatie_snelheid os ON sel3.sensor = os.sensor and sel3.timestamp_rounded=os.timestamp_rounded
                 LEFT JOIN v2_countaggregate_zone_count oc ON sel3.sensor = oc.sensor and sel3.timestamp_rounded=oc.timestamp_rounded),
    V1_en_V2_data_15min as  ( 
        SELECT v1_data.sensor,
        v1_data.timestamp_rounded,
        v1_data.total_count,
        v1_data.count_down,
        v1_data.count_up,
        v1_data.density_avg,
        v1_data.speed_avg,
        v1_data.basedonxmessages
        FROM v1_data
    UNION ALL
        SELECT v2_data.sensor,
        v2_data.timestamp_rounded,
        v2_data.total_count,
        v2_data.count_up,
        v2_data.count_down,
        v2_data.density_avg,
        v2_data.speed_avg,
        v2_data.basedonxmessages
    FROM v2_data        )     
                 
    , percentiles as (
             SELECT v.sensor,
                date_part('dow'::text, v.timestamp_rounded)::integer AS dayofweek,
                v.timestamp_rounded::time without time zone AS castedtimestamp,
                percentile_disc(0.1::double precision) WITHIN GROUP (ORDER BY v.total_count) AS total_count_p10,
                percentile_disc(0.2::double precision) WITHIN GROUP (ORDER BY v.total_count) AS total_count_p20,
                percentile_disc(0.5::double precision) WITHIN GROUP (ORDER BY v.total_count) AS total_count_p50,
                percentile_disc(0.8::double precision) WITHIN GROUP (ORDER BY v.total_count) AS total_count_p80,
                percentile_disc(0.9::double precision) WITHIN GROUP (ORDER BY v.total_count) AS total_count_p90,
                percentile_disc(0.1::double precision) WITHIN GROUP (ORDER BY v.count_down) AS count_down_p10,
                percentile_disc(0.2::double precision) WITHIN GROUP (ORDER BY v.count_down) AS count_down_p20,
                percentile_disc(0.5::double precision) WITHIN GROUP (ORDER BY v.count_down) AS count_down_p50,
                percentile_disc(0.8::double precision) WITHIN GROUP (ORDER BY v.count_down) AS count_down_p80,
                percentile_disc(0.9::double precision) WITHIN GROUP (ORDER BY v.count_down) AS count_down_p90,
                percentile_disc(0.1::double precision) WITHIN GROUP (ORDER BY v.count_up) AS count_up_p10,
                percentile_disc(0.2::double precision) WITHIN GROUP (ORDER BY v.count_up) AS count_up_p20,
                percentile_disc(0.5::double precision) WITHIN GROUP (ORDER BY v.count_up) AS count_up_p50,
                percentile_disc(0.8::double precision) WITHIN GROUP (ORDER BY v.count_up) AS count_up_p80,
                percentile_disc(0.9::double precision) WITHIN GROUP (ORDER BY v.count_up) AS count_up_p90,
                percentile_disc(0.2::double precision) WITHIN GROUP (ORDER BY v.density_avg) AS density_avg_p20,
                percentile_disc(0.5::double precision) WITHIN GROUP (ORDER BY v.density_avg) AS density_avg_p50,
                percentile_disc(0.8::double precision) WITHIN GROUP (ORDER BY v.density_avg) AS density_avg_p80,
                percentile_disc(0.2::double precision) WITHIN GROUP (ORDER BY v.speed_avg) AS speed_avg_p20,
                percentile_disc(0.5::double precision) WITHIN GROUP (ORDER BY v.speed_avg) AS speed_avg_p50,
                percentile_disc(0.8::double precision) WITHIN GROUP (ORDER BY v.speed_avg) AS speed_avg_p80
               FROM V1_en_V2_data_15min v
              WHERE v.timestamp_rounded >= (( SELECT now() - '1 year'::interval))
              GROUP BY v.sensor, (date_part('dow'::text, v.timestamp_rounded)), (v.timestamp_rounded::time without time zone))
     SELECT aq.sensor,
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
       FROM V1_en_V2_data_15min aq
         LEFT JOIN percentiles p ON aq.sensor::text = p.sensor::text AND date_part('dow'::text, aq.timestamp_rounded) = p.dayofweek::double precision AND aq.timestamp_rounded::time without time zone = p.castedtimestamp
      ORDER BY aq.sensor, aq.timestamp_rounded;""",

    'cmsa_15min_view_v7_realtime_predict': r"""
     WITH mat_view_updated AS
    ( --bepaal per sensor wanneer deze het laatst geupdate is in de materialized view met alle historie
             SELECT   m.sensor,
                      Max(m.timestamp_rounded) AS last_updated
             FROM     cmsa_15min_view_v7_materialized m
             WHERE    m.timestamp_rounded > Now() - interval '1 day'
             group BY m.sensor ) ,time_serie AS
    ( --maak time series vanaf de mat view update datum per sensor tot 1 uur na nu
           SELECT mat_view_updated.sensor,
                  generate_series(mat_view_updated.last_updated, now() + '01:00:00'::interval, '00:15:00'::interval) AS timestamp_rounded
           FROM   mat_view_updated ) , v2_selectie AS
    ( --bepaal de observaties in de selectie. Bij dubbele observaties moet de laatste worden gebruikt
           SELECT o.id,
                  o.sensor,
                  o.timestamp_start,
                  date_trunc('hour'::text, o.timestamp_start) + (date_part('minute'::text, o.timestamp_start)::INTEGER / 15)::DOUBLE PRECISION * '00:15:00'::interval AS timestamp_rounded
           FROM   telcameras_v2_observation o
           join   mat_view_updated u
           ON     o.sensor = u.sensor
           WHERE
                  --     id IN (SELECT id FROM(SELECT id,ROW_NUMBER() OVER(PARTITION BY sensor, timestamp_start ORDER BY sensor, timestamp_start, timestamp_message DESC )
                  --  AS row_num FROM telcameras_v2_observation ) t WHERE t.row_num = 1)
                  --   AND
                  o.timestamp_start > u.last_updated ) ,v2_sensor_15min_sel AS
    ( --maak een selectietabel met alleen sensoren en timestamp_rounded kwartieren verschil met time_serie is dat deze alle sensor-kwartier combinaties bevat
             SELECT   sensor,
                      timestamp_rounded
             FROM     v2_selectie
             GROUP BY sensor,
                      timestamp_rounded
             ORDER BY sensor,
                      timestamp_rounded ) , v2_observatie_snelheid AS ( WITH v2_observatie_persoon AS
    (-- bepaal de gemiddelde snelheid voor 3D camera's op basis van hoe lang mensen in beeld waren, voor 2D is het ongewogen.
           -- hier wordt dus het tijdstippen-deel van geom er uitgehaald en tot array gemaakt
           SELECT sel.sensor,
                  sel.timestamp_rounded,
                  pa.speed,
                  string_to_array(substr(pa.geom,position('('IN pa.geom)+1, (position(')'IN pa.geom)- position('('IN pa.geom)-1)), ' ' ) AS tijd_array
           FROM   telcameras_v2_personaggregate pa
           join   v2_selectie sel
           ON     pa.observation_timestamp_start=sel.timestamp_start
           AND    pa.observation_id = sel.id
           WHERE  pa.observation_timestamp_start > now() - interval '1 day'
           AND    pa.speed IS NOT NULL
           AND    pa.geom IS NOT NULL
           AND    pa.geom::text <> ''::text
           UNION ALL
           SELECT sel2.sensor,
                  sel2.timestamp_rounded,
                  pa.speed,
                  ARRAY['1',
                  '2'] AS tijd_array
           FROM   telcameras_v2_personaggregate pa
           join   v2_selectie sel2
           ON     pa.observation_timestamp_start=sel2.timestamp_start
           AND    pa.observation_id = sel2.id
           WHERE  pa.observation_timestamp_start > now() - interval '1 day'
           AND    pa.speed IS NOT NULL
           AND    (
                         pa.geom IS NULL
                  OR     pa.geom::text = ''::text) )
    -- nu word per sensor de som van de in 15min afgelegde afstand berekend als tijd*speed, en die wordt gedeeld door de som van de tijden in die 15 min
    SELECT   p.sensor,
             p.timestamp_rounded,
             CASE
                      WHEN SUM(tijd_array[cardinality(tijd_array)]::NUMERIC - tijd_array[1]::NUMERIC)>0 THEN (SUM(p.speed * (tijd_array[cardinality(tijd_array)]::NUMERIC - tijd_array[1]::NUMERIC)) / SUM(tijd_array[cardinality(tijd_array)]::NUMERIC - tijd_array[1]::NUMERIC))
                      ELSE 0
             END AS speed_avg
    FROM     v2_observatie_persoon p
    GROUP BY p.sensor,
             p.timestamp_rounded ) , v2_countaggregate_zone_count AS
    ( --lees de tellingen, en aggregeer op 15 minuten
             -- de vraag is of hier nog een correctie in moet voor kwartieren met te weinig berichten zoals dat voor V1 gemaakt was
             -- in elk geval zou dit misschien voor het laatste kwartier moeten, of een drempel voor dit laatste kwartier als het nog niet compleet is.
             SELECT   sel.sensor,
                      sel.timestamp_rounded,
                      max(c.azimuth)              AS azimuth,
                      SUM(c.count_in)             AS count_in,
                      SUM(c.count_out)            AS count_out,
                      SUM(c.count_in+c.count_out) AS total_count,
                      avg(c.count)                AS area_count,
                      max(c.area)                 AS area,
                      count(*)                    AS basedonxmessages
             FROM     telcameras_v2_countaggregate c
             join     v2_selectie sel
             ON       c.observation_id = sel.id
             AND      c.observation_timestamp_start=sel.timestamp_start
             GROUP BY sel.sensor,
                      sel.timestamp_rounded ) , aggregatedbyquarter AS
    (
              SELECT    sel3.sensor,
                        sel3.timestamp_rounded,
                        coalesce(oc.total_count::INTEGER, 0) AS total_count,
                        coalesce(oc.count_in::   INTEGER, 0) AS count_up,
                        coalesce(oc.count_out::  INTEGER, 0) AS count_down,
                        CASE
                                  WHEN oc.area IS NOT NULL
                                  AND       oc.area <> 0::DOUBLE PRECISION
                                  AND       oc.area_count IS NOT NULL
                                  AND       oc.area_count > 0 THEN oc.area_count::DOUBLE PRECISION / oc.area
                                  ELSE NULL::DOUBLE PRECISION
                        END AS density_avg,
                        os.speed_avg,
                        oc.basedonxmessages
              FROM      v2_sensor_15min_sel sel3
              left join v2_observatie_snelheid os
              ON        sel3.sensor = os.sensor
              AND       sel3.timestamp_rounded=os.timestamp_rounded
              left join v2_countaggregate_zone_count oc
              ON        sel3.sensor = oc.sensor
              AND       sel3.timestamp_rounded=oc.timestamp_rounded ) , percentiles AS
    (
             SELECT   a.sensor, -- haal de percentielen op uit de public.cmsa_15min_view_v7 view
                      date_part('dow'::text, a.timestamp_rounded) AS dayofweek,
                      a.timestamp_rounded::TIME without TIME zone AS castedtimestamp,
                      avg(a.total_count_p10)                      AS total_count_p10,
                      avg(a.total_count_p20)                      AS total_count_p20,
                      avg(a.total_count_p50)                      AS total_count_p50,
                      avg(a.total_count_p80)                      AS total_count_p80,
                      avg(a.total_count_p90)                      AS total_count_p90,
                      avg(a.count_down_p10)                       AS count_down_p10,
                      avg(a.count_down_p20)                       AS count_down_p20,
                      avg(a.count_down_p50)                       AS count_down_p50,
                      avg(a.count_down_p80)                       AS count_down_p80,
                      avg(a.count_down_p90)                       AS count_down_p90,
                      avg(a.count_up_p10)                         AS count_up_p10,
                      avg(a.count_up_p20)                         AS count_up_p20,
                      avg(a.count_up_p50)                         AS count_up_p50,
                      avg(a.count_up_p80)                         AS count_up_p80,
                      avg(a.count_up_p90)                         AS count_up_p90,
                      avg(a.density_avg_p20)                      AS density_avg_p20,
                      avg(a.density_avg_p50)                      AS density_avg_p50,
                      avg(a.density_avg_p80)                      AS density_avg_p80,
                      avg(a.speed_avg_p20)                        AS speed_avg_p20,
                      avg(a.speed_avg_p50)                        AS speed_avg_p50,
                      avg(a.speed_avg_p80)                        AS speed_avg_p80
             FROM     cmsa_15min_view_v7_materialized a
             WHERE    a.timestamp_rounded >=
                      (
                             SELECT now() - '8 day'::interval) -- beperk de scope tot alleen de laatste 8 dagen
             GROUP BY a.sensor,
                      (date_part('dow'::text, a.timestamp_rounded)),
                      (a.timestamp_rounded::TIME without TIME zone) ) ,laatste_2_uur_data AS
    ( --bepaal de laatste 8 kwartieren aan data
           SELECT rank_filter.*
           FROM   (
                           SELECT   *,
                                    rank() over ( PARTITION BY sensor ORDER BY timestamp_rounded DESC) AS bronnr
                           FROM     aggregatedbyquarter
                           WHERE    timestamp_rounded <= now() ) rank_filter
           WHERE  bronnr < 9 ) ,komende_2_uur_data AS
    ( -- de voorspelperiode wordt bepaald in time_serie en is nu 1 uur vooruit
           SELECT rank_filter.*
           FROM   (
                           SELECT   *,
                                    rank() over ( PARTITION BY sensor ORDER BY timestamp_rounded ASC) AS toepnr -- 1 is het eerste kwartier na nu
                           FROM     time_serie
                           WHERE    timestamp_rounded > now() ) rank_filter
           WHERE  toepnr < 9 ) ,alle_data_met_vc AS
    ( --pas nu per sensor de coeffiecienten en interceptwaardes toe op een cartesisch product van de laaste 2 uur en de komende kwartieren.
              SELECT    d.sensor,
                        d.timestamp_rounded_bron,
                        d.total_count,
                        d.basedonxmessages,
                        d.bronnr,
                        d.toepnr,
                        vi.intercept_waarde,
                        vc.coefficient_waarde,
                        d.timestamp_rounded_toep
              FROM      (
                               SELECT b.sensor,
                                      b.timestamp_rounded AS timestamp_rounded_bron,
                                      b.total_count,
                                      b.basedonxmessages,
                                      b.bronnr,
                                      k.toepnr,
                                      k.timestamp_rounded AS timestamp_rounded_toep
                               FROM   laatste_2_uur_data b
                               join   komende_2_uur_data k
                               ON     b.sensor=k.sensor ) d
              left join peoplemeasurement_voorspelintercept vi
              ON        vi.sensor=d.sensor
              AND       vi.toepassings_kwartier_volgnummer=d.toepnr
              left join peoplemeasurement_voorspelcoefficient vc
              ON        vc.sensor=d.sensor
              AND       vc.bron_kwartier_volgnummer=d.bronnr
              AND       vc.toepassings_kwartier_volgnummer=d.toepnr
              ORDER BY  d.sensor,
                        toepnr,
                        bronnr ) ,voorspel_berekening AS
    (
              SELECT    vc.sensor,
                        vc.timestamp_rounded_toep,
                        vc.toepnr,
                        total_count_voorspeld+vi.intercept_waarde AS total_count_forecast
              FROM      (
                                 SELECT   sensor,
                                          timestamp_rounded_toep,
                                          toepnr,
                                          SUM(total_count*coefficient_waarde) AS total_count_voorspeld
                                 FROM     alle_data_met_vc
                                 GROUP BY sensor,
                                          timestamp_rounded_toep,
                                          toepnr
                                 ORDER BY sensor,
                                          timestamp_rounded_toep,
                                          toepnr) vc
              left join peoplemeasurement_voorspelintercept vi
              ON        vi.sensor=vc.sensor
              AND       vi.toepassings_kwartier_volgnummer=vc.toepnr )
    SELECT    s.sensor, -- 'hang' de data (15  minuten waarnemingen,de percentielen en de voorspelling)op aan de time series.
              s.timestamp_rounded,
              coalesce(aq.total_count, 0::NUMERIC) AS total_count,
              vb.total_count_forecast,
              coalesce(aq.count_down, 0::NUMERIC)           AS count_down,
              coalesce(aq.count_up, 0::  NUMERIC)           AS count_up,
              coalesce(aq.density_avg, 0::DOUBLE PRECISION) AS density_avg,
              coalesce(aq.speed_avg, 0::       NUMERIC)            AS speed_avg,
              coalesce(aq.basedonxmessages, 0::bigint)             AS basedonxmessages,
              coalesce(p.total_count_p10, 0::  NUMERIC)            AS total_count_p10,
              coalesce(p.total_count_p20, 0::  NUMERIC)            AS total_count_p20,
              coalesce(p.total_count_p50, 0::  NUMERIC)            AS total_count_p50,
              coalesce(p.total_count_p80, 0::  NUMERIC)            AS total_count_p80,
              coalesce(p.total_count_p90, 0::  NUMERIC)            AS total_count_p90,
              coalesce(p.count_down_p10, 0::   NUMERIC)            AS count_down_p10,
              coalesce(p.count_down_p20, 0::   NUMERIC)            AS count_down_p20,
              coalesce(p.count_down_p50, 0::   NUMERIC)            AS count_down_p50,
              coalesce(p.count_down_p80, 0::   NUMERIC)            AS count_down_p80,
              coalesce(p.count_down_p90, 0::   NUMERIC)            AS count_down_p90,
              coalesce(p.count_up_p10, 0::     NUMERIC)            AS count_up_p10,
              coalesce(p.count_up_p20, 0::     NUMERIC)            AS count_up_p20,
              coalesce(p.count_up_p50, 0::     NUMERIC)            AS count_up_p50,
              coalesce(p.count_up_p80, 0::     NUMERIC)            AS count_up_p80,
              coalesce(p.count_up_p90, 0::     NUMERIC)            AS count_up_p90,
              coalesce(p.density_avg_p20, 0::DOUBLE PRECISION)     AS density_avg_p20,
              coalesce(p.density_avg_p50, 0::DOUBLE PRECISION)     AS density_avg_p50,
              coalesce(p.density_avg_p80, 0::DOUBLE PRECISION)     AS density_avg_p80,
              coalesce(p.speed_avg_p20, 0::NUMERIC)                AS speed_avg_p20,
              coalesce(p.speed_avg_p50, 0::NUMERIC)                AS speed_avg_p50,
              coalesce(p.speed_avg_p80, 0::NUMERIC)                AS speed_avg_p80
    FROM      time_serie s
    left join aggregatedbyquarter aq
    ON        s.sensor::text = aq.sensor::text
    AND       aq.timestamp_rounded = s.timestamp_rounded
    left join percentiles p
    ON        aq.sensor::text = p.sensor::text
    AND       date_part('dow'::text, aq.timestamp_rounded) = p.dayofweek
    AND       aq.timestamp_rounded::TIME without TIME zone = p.castedtimestamp
    left join voorspel_berekening vb
    ON        vb.sensor=s.sensor
    AND       s.timestamp_rounded = vb.timestamp_rounded_toep
    WHERE     s.sensor = 'GAWW-04'
    ORDER BY  s.sensor,
              s.timestamp_rounded; 
    """,
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
