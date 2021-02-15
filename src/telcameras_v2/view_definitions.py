VIEW_STRINGS = {
    # NOTE: the regex in these queries cause a DeprecationWarning: invalid escape sequence
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
            AND o.timestamp_start > now() - INTERVAL '1 YEAR'
            AND o.timestamp_start < now() - INTERVAL '18 MINUTES')
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
    FROM v2_data)
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


    'cmsa_15min_view_v7_realtime_predict': r"""CREATE VIEW cmsa_15min_view_v7_realtime_predict AS
     WITH mat_view_updated AS (
             SELECT m.sensor,
                max(m.timestamp_rounded) + '00:15:00'::interval AS last_updated
               FROM cmsa_15min_view_v7_materialized m
              WHERE m.timestamp_rounded > (now() - '3 days'::interval)
              GROUP BY m.sensor
            ), time_serie AS (
             SELECT mat_view_updated.sensor,
                generate_series(mat_view_updated.last_updated, now() + '01:00:00'::interval, '00:15:00'::interval) AS timestamp_rounded
               FROM mat_view_updated
            ), v2_selectie AS (
             SELECT o.id,
                o.sensor,
                o.timestamp_start,
                date_trunc('hour'::text, o.timestamp_start) + (date_part('minute'::text, o.timestamp_start)::integer / 15)::double precision * '00:15:00'::interval AS timestamp_rounded
               FROM telcameras_v2_observation o
                 LEFT JOIN mat_view_updated u ON o.sensor::text = u.sensor::text
              WHERE (o.id IN ( SELECT t.id
                       FROM ( SELECT telcameras_v2_observation.id,
                                row_number() OVER (PARTITION BY telcameras_v2_observation.sensor, telcameras_v2_observation.timestamp_start ORDER BY telcameras_v2_observation.sensor, telcameras_v2_observation.timestamp_start, telcameras_v2_observation.timestamp_message DESC) AS row_num
                               FROM telcameras_v2_observation WHERE timestamp_start > (now() - '1 days'::interval)) t
                      WHERE t.row_num = 1)) AND timestamp_start > (now() - '1 days'::interval) AND (o.timestamp_start > u.last_updated OR u.last_updated IS NULL)
            ), v2_sensor_15min_sel AS (
             SELECT v2_selectie.sensor,
                v2_selectie.timestamp_rounded
               FROM v2_selectie
              GROUP BY v2_selectie.sensor, v2_selectie.timestamp_rounded
              ORDER BY v2_selectie.sensor, v2_selectie.timestamp_rounded
            ), v2_observatie_snelheid AS (
             WITH v2_observatie_persoon AS (
                     SELECT sel.sensor,
                        sel.timestamp_rounded,
                        pa.speed,
                        string_to_array(substr(pa.geom::text, "position"(pa.geom::text, '('::text) + 1, "position"(pa.geom::text, ')'::text) - "position"(pa.geom::text, '('::text) - 1), ' '::text) AS tijd_array
                       FROM telcameras_v2_personaggregate pa
                         JOIN v2_selectie sel ON pa.observation_id = sel.id AND pa.observation_timestamp_start = sel.timestamp_start
                      WHERE pa.observation_timestamp_start > (now() - '1 days'::interval) AND pa.speed IS NOT NULL AND pa.geom IS NOT NULL AND pa.geom::text <> ''::text
                    UNION ALL
                     SELECT sel2.sensor,
                        sel2.timestamp_rounded,
                        pa.speed,
                        ARRAY['1'::text, '2'::text] AS tijd_array
                       FROM telcameras_v2_personaggregate pa
                         JOIN v2_selectie sel2 ON pa.observation_id = sel2.id AND pa.observation_timestamp_start = sel2.timestamp_start
                      WHERE pa.observation_timestamp_start > (now() - '1 days'::interval) AND pa.speed IS NOT NULL AND (pa.geom IS NULL OR pa.geom::text = ''::text)
                    )
             SELECT p_1.sensor,
                p_1.timestamp_rounded,
                    CASE
                        WHEN sum(p_1.tijd_array[cardinality(p_1.tijd_array)]::numeric - p_1.tijd_array[1]::numeric) > 0::numeric THEN sum(p_1.speed * (p_1.tijd_array[cardinality(p_1.tijd_array)]::numeric - p_1.tijd_array[1]::numeric)::double precision) / sum(p_1.tijd_array[cardinality(p_1.tijd_array)]::numeric - p_1.tijd_array[1]::numeric)::double precision
                        ELSE 0::double precision
                    END AS speed_avg
               FROM v2_observatie_persoon p_1
              GROUP BY p_1.sensor, p_1.timestamp_rounded
            ), v2_countaggregate_zone_count AS (
             SELECT sel.sensor,
                sel.timestamp_rounded,
                max(c.azimuth) AS azimuth,
                sum(c.count_in) AS count_in,
                sum(c.count_out) AS count_out,
                sum(c.count_in + c.count_out) AS total_count,
                avg(c.count) AS area_count,
                max(c.area) AS area,
                count(*) AS basedonxmessages
               FROM telcameras_v2_countaggregate c
                 JOIN v2_selectie sel ON c.observation_id = sel.id AND c.observation_timestamp_start = sel.timestamp_start
                WHERE c.observation_timestamp_start > (now() - '1 days'::interval)
              GROUP BY sel.sensor, sel.timestamp_rounded
            ), aggregatedbyquarter AS (
             SELECT sel3.sensor,
                sel3.timestamp_rounded,
                COALESCE(oc.total_count::integer, 0) AS total_count,
                COALESCE(oc.count_in::integer, 0) AS count_up,
                COALESCE(oc.count_out::integer, 0) AS count_down,
                    CASE
                        WHEN oc.area IS NOT NULL AND oc.area <> 0::double precision AND oc.area_count IS NOT NULL AND oc.area_count > 0::numeric THEN oc.area_count::double precision / oc.area
                        ELSE NULL::double precision
                    END AS density_avg,
                os.speed_avg,
                oc.basedonxmessages
               FROM v2_sensor_15min_sel sel3
                 LEFT JOIN v2_observatie_snelheid os ON sel3.sensor::text = os.sensor::text AND sel3.timestamp_rounded = os.timestamp_rounded
                 LEFT JOIN v2_countaggregate_zone_count oc ON sel3.sensor::text = oc.sensor::text AND sel3.timestamp_rounded = oc.timestamp_rounded
            ), percentiles AS (
             SELECT a.sensor,
                date_part('dow'::text, a.timestamp_rounded) AS dayofweek,
                a.timestamp_rounded::time without time zone AS castedtimestamp,
                avg(a.total_count_p10) AS total_count_p10,
                avg(a.total_count_p20) AS total_count_p20,
                avg(a.total_count_p50) AS total_count_p50,
                avg(a.total_count_p80) AS total_count_p80,
                avg(a.total_count_p90) AS total_count_p90,
                avg(a.count_down_p10) AS count_down_p10,
                avg(a.count_down_p20) AS count_down_p20,
                avg(a.count_down_p50) AS count_down_p50,
                avg(a.count_down_p80) AS count_down_p80,
                avg(a.count_down_p90) AS count_down_p90,
                avg(a.count_up_p10) AS count_up_p10,
                avg(a.count_up_p20) AS count_up_p20,
                avg(a.count_up_p50) AS count_up_p50,
                avg(a.count_up_p80) AS count_up_p80,
                avg(a.count_up_p90) AS count_up_p90,
                avg(a.density_avg_p20) AS density_avg_p20,
                avg(a.density_avg_p50) AS density_avg_p50,
                avg(a.density_avg_p80) AS density_avg_p80,
                avg(a.speed_avg_p20) AS speed_avg_p20,
                avg(a.speed_avg_p50) AS speed_avg_p50,
                avg(a.speed_avg_p80) AS speed_avg_p80
               FROM cmsa_15min_view_v7_materialized a
              WHERE a.timestamp_rounded >= (( SELECT now() - '8 days'::interval))
              GROUP BY a.sensor, (date_part('dow'::text, a.timestamp_rounded)), (a.timestamp_rounded::time without time zone)
            ), laatste_2_uur_data AS (
             SELECT rank_filter.sensor,
                rank_filter.timestamp_rounded,
                rank_filter.total_count,
                rank_filter.basedonxmessages,
                rank_filter.bronnr
               FROM ( SELECT a.sensor,
                        a.timestamp_rounded,
                        a.total_count * 15 / a.basedonxmessages AS total_count,
                        a.basedonxmessages,
                        rank() OVER (PARTITION BY a.sensor ORDER BY a.timestamp_rounded DESC) AS bronnr
                       FROM aggregatedbyquarter a
                      WHERE a.timestamp_rounded < (now() - '00:20:00'::interval) AND a.timestamp_rounded > (now() - '02:20:00'::interval) AND a.basedonxmessages >= 10) rank_filter
              WHERE rank_filter.bronnr < 9
            ), laatste_2_uur_data_compleet AS (
             SELECT laatste_2_uur_data.sensor,
                laatste_2_uur_data.timestamp_rounded,
                laatste_2_uur_data.total_count,
                laatste_2_uur_data.basedonxmessages,
                laatste_2_uur_data.bronnr
               FROM laatste_2_uur_data
              WHERE (laatste_2_uur_data.sensor::text IN ( SELECT laatste_2_uur_data_1.sensor
                       FROM laatste_2_uur_data laatste_2_uur_data_1
                      GROUP BY laatste_2_uur_data_1.sensor
                     HAVING count(*) = 8))
            ), komende_2_uur_data AS (
             SELECT rank_filter.sensor,
                rank_filter.timestamp_rounded,
                rank_filter.toepnr
               FROM ( SELECT time_serie.sensor,
                        time_serie.timestamp_rounded,
                        rank() OVER (PARTITION BY time_serie.sensor ORDER BY time_serie.timestamp_rounded) AS toepnr
                       FROM time_serie
                      WHERE time_serie.timestamp_rounded > (now() - '00:20:00'::interval)) rank_filter
              WHERE rank_filter.toepnr < 9
            ), alle_data_met_vc AS (
             SELECT d.sensor,
                d.timestamp_rounded_bron,
                d.total_count,
                d.basedonxmessages,
                d.bronnr,
                d.toepnr,
                vi.intercept_waarde,
                vc.coefficient_waarde,
                d.timestamp_rounded_toep
               FROM ( SELECT b.sensor,
                        b.timestamp_rounded AS timestamp_rounded_bron,
                        b.total_count,
                        b.basedonxmessages,
                        b.bronnr,
                        k.toepnr,
                        k.timestamp_rounded AS timestamp_rounded_toep
                       FROM laatste_2_uur_data_compleet b
                         JOIN komende_2_uur_data k ON b.sensor::text = k.sensor::text) d
                 LEFT JOIN peoplemeasurement_voorspelintercept vi ON vi.sensor::text = d.sensor::text AND vi.toepassings_kwartier_volgnummer = d.toepnr
                 LEFT JOIN peoplemeasurement_voorspelcoefficient vc ON vc.sensor::text = d.sensor::text AND vc.bron_kwartier_volgnummer = d.bronnr AND vc.toepassings_kwartier_volgnummer = d.toepnr
              ORDER BY d.sensor, d.toepnr, d.bronnr
            )
            , voorspel_berekening AS (
             SELECT vc.sensor,
                vc.timestamp_rounded_toep,
                vc.toepnr,
                vc.total_count_voorspeld + vi.intercept_waarde AS total_count_forecast
               FROM ( SELECT alle_data_met_vc.sensor,
                        alle_data_met_vc.timestamp_rounded_toep,
                        alle_data_met_vc.toepnr,
                        sum(alle_data_met_vc.total_count::double precision * alle_data_met_vc.coefficient_waarde) AS total_count_voorspeld
                       FROM alle_data_met_vc
                      GROUP BY alle_data_met_vc.sensor, alle_data_met_vc.timestamp_rounded_toep, alle_data_met_vc.toepnr
                      ORDER BY alle_data_met_vc.sensor, alle_data_met_vc.timestamp_rounded_toep, alle_data_met_vc.toepnr) vc
                 LEFT JOIN peoplemeasurement_voorspelintercept vi ON vi.sensor::text = vc.sensor::text AND vi.toepassings_kwartier_volgnummer = vc.toepnr
            )
     SELECT s.sensor,
        s.timestamp_rounded,
        COALESCE(aq.total_count::numeric, 0::numeric) AS total_count,
        vb.total_count_forecast,
        COALESCE(aq.count_down::numeric, 0::numeric) AS count_down,
        COALESCE(aq.count_up::numeric, 0::numeric) AS count_up,
        COALESCE(aq.density_avg, 0::double precision) AS density_avg,
        COALESCE(aq.speed_avg, 0::numeric::double precision) AS speed_avg,
        COALESCE(aq.basedonxmessages, 0::bigint) AS basedonxmessages,
        COALESCE(p.total_count_p10, 0::numeric) AS total_count_p10,
        COALESCE(p.total_count_p20, 0::numeric) AS total_count_p20,
        COALESCE(p.total_count_p50, 0::numeric) AS total_count_p50,
        COALESCE(p.total_count_p80, 0::numeric) AS total_count_p80,
        COALESCE(p.total_count_p90, 0::numeric) AS total_count_p90,
        COALESCE(p.count_down_p10, 0::numeric) AS count_down_p10,
        COALESCE(p.count_down_p20, 0::numeric) AS count_down_p20,
        COALESCE(p.count_down_p50, 0::numeric) AS count_down_p50,
        COALESCE(p.count_down_p80, 0::numeric) AS count_down_p80,
        COALESCE(p.count_down_p90, 0::numeric) AS count_down_p90,
        COALESCE(p.count_up_p10, 0::numeric) AS count_up_p10,
        COALESCE(p.count_up_p20, 0::numeric) AS count_up_p20,
        COALESCE(p.count_up_p50, 0::numeric) AS count_up_p50,
        COALESCE(p.count_up_p80, 0::numeric) AS count_up_p80,
        COALESCE(p.count_up_p90, 0::numeric) AS count_up_p90,
        COALESCE(p.density_avg_p20, 0::double precision) AS density_avg_p20,
        COALESCE(p.density_avg_p50, 0::double precision) AS density_avg_p50,
        COALESCE(p.density_avg_p80, 0::double precision) AS density_avg_p80,
        COALESCE(p.speed_avg_p20, 0::numeric::double precision) AS speed_avg_p20,
        COALESCE(p.speed_avg_p50, 0::numeric::double precision) AS speed_avg_p50,
        COALESCE(p.speed_avg_p80, 0::numeric::double precision) AS speed_avg_p80
       FROM time_serie s
         LEFT JOIN aggregatedbyquarter aq ON s.sensor::text = aq.sensor::text AND aq.timestamp_rounded = s.timestamp_rounded
         LEFT JOIN percentiles p ON aq.sensor::text = p.sensor::text AND date_part('dow'::text, aq.timestamp_rounded) = p.dayofweek AND aq.timestamp_rounded::time without time zone = p.castedtimestamp
         LEFT JOIN voorspel_berekening vb ON vb.sensor::text = s.sensor::text AND s.timestamp_rounded = vb.timestamp_rounded_toep
      ORDER BY s.sensor, s.timestamp_rounded;
    """,


    'cmsa_15min_view_v8': r"""
      CREATE VIEW cmsa_15min_view_v8 AS

        with v2_feed_start_date as (
            select
            sensor
            , min(timestamp_start)  as start_of_feed
            from telcameras_v2_observation
            group by sensor 
        )

        , v1_data_uniek as (
            select
              a.sensor
            , a."timestamp"
            , max(a.id::text)       as idt
            from peoplemeasurement_peoplemeasurement    as a
            left join v2_feed_start_date                as fsd      on fsd.sensor::text = a.sensor::text
            where 1=1
            and (
                a."timestamp" < fsd.start_of_feed
                or fsd.start_of_feed is null
            )
            group by
              a.sensor
            , a."timestamp" 
        )

        , v1_data_sel as (
            select
              dp.sensor
            , dp."timestamp"
            ,       date_trunc('hour'::text, dp."timestamp") 
                + (date_part('minute'::text, dp."timestamp")::integer / 15)::double precision 
                * '00:15:00'::interval          as timestamp_rounded
            , 1                                 as aantal
            , dp.details
            from peoplemeasurement_peoplemeasurement    as dp
            join v1_data_uniek                          as csdu     on  dp.id::text = csdu.idt
                                                                    and dp."timestamp" = csdu."timestamp" 
        )

        , v1_data as (
            select
              ds.sensor
            , ds.timestamp_rounded
            , count(distinct ds."timestamp")                                                                                                                                 as basedonxmessages
            ,       coalesce(sum((detail_elems.value ->> 'count'::text)::integer)   filter (where (detail_elems.value ->> 'direction'::text) = 'down'::text), 0::bigint) 
                + coalesce(sum((detail_elems.value ->> 'count'::text)::integer)     filter (where (detail_elems.value ->> 'direction'::text) = 'up'::text), 0::bigint)       as total_count
            , coalesce(sum((detail_elems.value ->> 'count'::text)::integer)         filter (where (detail_elems.value ->> 'direction'::text) = 'down'::text), 0::bigint)     as count_down
            , coalesce(sum((detail_elems.value ->> 'count'::text)::integer)         filter (where (detail_elems.value ->> 'direction'::text) = 'up'::text), 0::bigint)       as count_up
            , avg((detail_elems.value ->> 'count'::text)::numeric)                  filter (where (detail_elems.value ->> 'direction'::text) = 'density'::text)              as density_avg
            , avg((detail_elems.value ->> 'count'::text)::numeric)                  filter (where (detail_elems.value ->> 'direction'::text) = 'speed'::text)                as speed_avg
            from v1_data_sel    as ds
            , lateral jsonb_array_elements(ds.details) detail_elems(value)
            group by
              ds.sensor
            , ds.timestamp_rounded
            order by
              ds.sensor
            , ds.timestamp_rounded 
        )

        , v2_selectie as (
            select
              id
            , sensor
            , timestamp_start
            ,       date_trunc('hour'::text, timestamp_start) 
                + (date_part('minute'::text, timestamp_start)::integer / 15)::double precision 
                * '00:15:00'::interval                                                              as timestamp_rounded
            , 1                                                                                     as aantal
            from telcameras_v2_observation
            where (
                id in (
                    select
                      t.id
                    from (
                        select
                          id
                        , row_number() over (
                                partition by 
                                  sensor
                                , timestamp_start
                                order by
                                  sensor
                                , timestamp_start
                                , timestamp_message desc
                        ) as row_num
                        from telcameras_v2_observation
                    ) as t
                    where t.row_num = 1
                )
            )
            and timestamp_start > (now() - '1 year'::interval)
            and timestamp_start < (now() - '00:18:00'::interval) 
        )

        , v2_sensor_15min_sel as (
            select
              sensor
            , timestamp_rounded
            , sum(aantal)            as basedonxmessages
            from v2_selectie
            group by
              sensor
            , timestamp_rounded
            order by
              sensor
            , timestamp_rounded
        )

        , v2_observatie_snelheid as (
            with v2_observatie_persoon as (
                select
                  sel.sensor
                , sel.timestamp_rounded
                , pa.speed
                , string_to_array(substr(pa.geom, "position"(pa.geom, '('::text) + 1, "position"(pa.geom, ')'::text) - "position"(pa.geom, '('::text) - 1), ' '::text)  as tijd_array
                from telcameras_v2_personaggregate    as pa
                join v2_selectie                      as sel    on  pa.observation_id = sel.id
                                                                and pa.observation_timestamp_start = sel.timestamp_start
                where 1=1
                and pa.speed is not null
                and pa.geom is not null
                and pa.geom <> ''::text

                union all

                select
                  sel2.sensor
                , sel2.timestamp_rounded
                , pa.speed
                , array['1'::text, '2'::text]    as tijd_array
                from telcameras_v2_personaggregate      as pa
                join v2_selectie                        as sel2     on  pa.observation_id = sel2.id
                                                                    and pa.observation_timestamp_start = sel2.timestamp_start
                where 1=1
                and pa.speed is not null
                and (
                    pa.geom is null
                    or pa.geom = ''::text
                )
            )

            select
              sensor
            , timestamp_rounded
            , case
                when sum(tijd_array[cardinality(tijd_array)]::numeric - tijd_array[1]::numeric) > 0::numeric 
                    then      sum(speed * (tijd_array[cardinality(tijd_array)]::numeric - tijd_array[1]::numeric)::double precision) 
                            / sum(tijd_array[cardinality(tijd_array)]::numeric - tijd_array[1]::numeric)::double precision
                else 0::double precision
            end                                    as speed_avg
            from v2_observatie_persoon
            group by
              sensor
            , timestamp_rounded
        )

        , v2_countaggregate_zone_count as (
            select
              sel.sensor
            , sel.timestamp_rounded
            , max(c.azimuth)                    as azimuth
            , sum(c.count_in)                   as count_in
            , sum(c.count_out)                  as count_out
            , sum(c.count_in + c.count_out)     as total_count
            , avg(c.count)                      as area_count
            , max(c.area)                       as area
            from telcameras_v2_countaggregate       as c
            join v2_selectie                        as sel  on  c.observation_id = sel.id
                                                            and c.observation_timestamp_start = sel.timestamp_start
            group by
              sel.sensor
            , sel.timestamp_rounded 
        )

        , v2_data as (
            select
              sel3.sensor
            , sel3.timestamp_rounded
            , case
                when left(sel3.sensor, 4) in ('GADM', 'GAMM')           -- This filter applies to zone sensors for wich only the area_count is filled 
                then coalesce(oc.area_count::integer, 0)
                else coalesce(oc.total_count::integer, 0)
              end                                   as total_count
            , coalesce(oc.count_in::integer, 0)     as count_up
            , coalesce(oc.count_out::integer, 0)    as count_down
            , case
                when    oc.area is not null
                    and oc.area <> 0::double precision
                    and oc.area_count is not null
                    and oc.area_count > 0::numeric 
                        then oc.area_count::double precision / oc.area
                else null::double precision
              end                                   as density_avg
            , os.speed_avg
            , sel3.basedonxmessages
            from v2_sensor_15min_sel                as sel3
            left join v2_observatie_snelheid        as os       on  sel3.sensor::text = os.sensor::text
                                                                and sel3.timestamp_rounded = os.timestamp_rounded
            left join v2_countaggregate_zone_count  as oc       on  sel3.sensor::text = oc.sensor::text
                                                                and sel3.timestamp_rounded = oc.timestamp_rounded 
        ),

        v3_selectie as (
            /* V3 selection, HIG data */    
            /* 
            * Some observation records do have duplicates, for example id 1701379 for sensor CMSA-GAWW-16 (unique key = sensor + timestamp)
            * In these cases the last record (based on the create_at field) is taken, assuming these are better (corrections).
            *
            * Q = What are de exact definitions for the timestamp and created_at fields?
            * A = ...?
            *
            * Q = There are observations for 1 sensor with a different long/lat, for example sensor CMSA-GAWW-14, date 2021-01-06 vs. 2021-01-19?
            * A = In these cases the lat and long were adjusted during the test phase to get better insights.
            * 
            * Q = Why take only data from last year and exclude last 18 minutes?
            * A = ...?
            **/
            select
              o.id            as observation_id
            , o.sensor
            , o.timestamp
            , date_trunc('hour'::text, o.timestamp) + (date_part('minute'::text, o.timestamp)::integer / 15)::double precision * '00:15:00'::interval as timestamp_rounded
            , 1 as aantal
            , density
            from telcameras_v3_observation as o
            where (
                o.id in (                                       -- If multiple rows are present (based on sensor + timestamp) then pick last one based on latest date in create_at field
                    select
                      t.id
                    from (
                        select
                          id
                        , row_number() over (
                            partition by 
                              sensor
                            , "timestamp"
                            order by   
                              sensor
                            , timestamp
                            , created_at desc
                        ) as row_num
                        from telcameras_v3_observation
                    ) t
                    where t.row_num = 1
                )
            )
            and o.timestamp > (now() - '1 year'::interval)      -- Retreive only data from the last year (based on current timestamp)
            and o.timestamp < (now() - '00:18:00'::interval)    -- Exclude last 18 minutes
        )

        , v3_sensor_15min_sel as (
            select 
              sel.sensor                                                    -- name of sensor
            , sel.timestamp_rounded                                         -- the quarter to which this data applies
            , sum(aantal)                       as basedonxobservations     -- number of observations for specifc sensor (should be 15, 1 per minute)
            , sum(grpagg.count)                 as count                    -- number of counted objects (pedestrians/cyclist) within the quarter for specific azimuth (direction)
            , sum(sel.density) / sum(aantal)    as density_avg              -- calculate the average density by summing the density for all observations within the specific quarter and divide this by the count of observations (should be 15, 1 per minute)
            , grpagg.azimuth                                                -- the direction in degrees
            , row_number() over (
                partition by
                  sel.sensor
                , sel.timestamp_rounded
                order by 
                grpagg.azimuth
              )                                 as azimuth_seqence_number   -- set ordernumber by azimuth, causing number 1 is always the same azimuth (needed to determine up/down direction)
            , sum(grpagg.cumulative_distance)   as cumulative_distance      -- sum over the cumulative distance in meters for the relevant quarter
            , sum(grpagg.cumulative_time)       as cumulative_time          -- sum over the cumulative time in meters for the relevant quarter
         -- , sum(grpagg.median_speed)          as median_speed             -- sum over the median speed in meters/seconds, not needed because this median_speed is coming from the observation and therefore is per 1 minute
         --                                                                 -- so for a better calculation we use a new calculation with cumulative_distance / cumulative_time
            from telcameras_v3_groupaggregate       as grpagg
            inner join v3_selectie                  as sel      on grpagg.observation_id = sel.observation_id
            where 1=1
            and grpagg.observation_id in (
                select observation_id 
                from v3_selectie
            )
            group by
              sel.sensor
            , sel.timestamp_rounded
            , grpagg.azimuth
            order by 
              sel.sensor
            , sel.timestamp_rounded
        )

        , v3_data as (
            select
              up.sensor                                                     -- name of sensor
            , up.timestamp_rounded                                          -- the quarter to which this data applies
            , up.basedonxobservations                                       -- number of observations for specifc sensor (should be 15, 1 per minute)
            , up.density_avg                                                -- average density over the specific quarter, don't sum the up an down azimuth (directions) because density is coming from the observation table wich doesn't contain azimuth
            , up.count + down.count             as total_count              -- total count (wich contains both azimuth directions)
            , up.cumulative_distance 
                + down.cumulative_distance      as cumulative_distance      -- cumulative distance (wich contains both azimuth directions)
            , up.cumulative_time 
                + down.cumulative_time          as cumulative_time          -- cumulative time (wich contains both azimuth directions)
            , (   up.cumulative_distance 
                + down.cumulative_distance
              )
                /    
              nullif(
                  up.cumulative_time 
                + down.cumulative_time
                , 0
              )                                 as speed_avg                -- average speed 
            /* direction 1 */
         -- , up.azimuth                                                    -- the first azimuth, direction in degrees (up)
            , up.count                          as count_up                 -- count for azimuth nr.1 (direction 1)
         -- , up.cumulative_distance                                        -- cumulative distance for azimuth 1, drection in degrees (up)
         -- , up.cumulative_time                                            -- cumulative time for azimuth 1, drection in degrees (up)
         -- , up.median_speed                   as median_speed_up          -- median speed for azimuth 1, drection in degrees (up)
            /* direction 2 */
         --  , down.azimuth                                                 -- the second azimuth, direction in degrees (down)
            , down.count                        as count_down               -- count for azimuth nr.2 (direction 2)
         -- , down.cumulative_distance                                      -- cumulative distance for azimuth 2, drection in degrees (down)
         -- , down.cumulative_time                                          -- cumulative time for azimuth 2, drection in degrees (down)
         -- , down.median_speed                 as median_speed_down        -- median speed for azimuth 1, drection in degrees (up)
            from v3_sensor_15min_sel        as up
            inner join v3_sensor_15min_sel  as down     on  up.sensor = down.sensor
                                                        and up.timestamp_rounded = down.timestamp_rounded
                                                        and down.azimuth_seqence_number = 2
            where 1=1
            and up.azimuth_seqence_number = 1                        
        )

        , v1_v2_en_v3_data_15min as (
            select 
            v1_data.sensor,
            v1_data.timestamp_rounded,
            v1_data.total_count,
            v1_data.count_down,
            v1_data.count_up,
            v1_data.density_avg,
            v1_data.speed_avg,
            v1_data.basedonxmessages
            from v1_data

            union all

            select 
            v2_data.sensor,
            v2_data.timestamp_rounded,
            v2_data.total_count,
            v2_data.count_up,
            v2_data.count_down,
            v2_data.density_avg,
            v2_data.speed_avg,
            v2_data.basedonxmessages
            from v2_data

            union all

            select 
            v3_data.sensor,
            v3_data.timestamp_rounded,
            v3_data.total_count,
            v3_data.count_up,
            v3_data.count_down,
            v3_data.density_avg,
            v3_data.speed_avg,
            v3_data.basedonxobservations    as basedonxmessages
            from v3_data
        )

        , percentiles as (
            select
              v.sensor
            , date_part('dow'::text, v.timestamp_rounded)::integer                              as dayofweek
            , v.timestamp_rounded::time without time zone                                       as castedtimestamp
            , percentile_disc(0.1::double precision) within group (order by v.total_count)      as total_count_p10
            , percentile_disc(0.2::double precision) within group (order by v.total_count)      as total_count_p20
            , percentile_disc(0.5::double precision) within group (order by v.total_count)      as total_count_p50
            , percentile_disc(0.8::double precision) within group (order by v.total_count)      as total_count_p80
            , percentile_disc(0.9::double precision) within group (order by v.total_count)      as total_count_p90
            , percentile_disc(0.1::double precision) within group (order by v.count_down)       as count_down_p10
            , percentile_disc(0.2::double precision) within group (order by v.count_down)       as count_down_p20
            , percentile_disc(0.5::double precision) within group (order by v.count_down)       as count_down_p50
            , percentile_disc(0.8::double precision) within group (order by v.count_down)       as count_down_p80
            , percentile_disc(0.9::double precision) within group (order by v.count_down)       as count_down_p90
            , percentile_disc(0.1::double precision) within group (order by v.count_up)         as count_up_p10
            , percentile_disc(0.2::double precision) within group (order by v.count_up)         as count_up_p20
            , percentile_disc(0.5::double precision) within group (order by v.count_up)         as count_up_p50
            , percentile_disc(0.8::double precision) within group (order by v.count_up)         as count_up_p80
            , percentile_disc(0.9::double precision) within group (order by v.count_up)         as count_up_p90
            , percentile_disc(0.2::double precision) within group (order by v.density_avg)      as density_avg_p20
            , percentile_disc(0.5::double precision) within group (order by v.density_avg)      as density_avg_p50
            , percentile_disc(0.8::double precision) within group (order by v.density_avg)      as density_avg_p80
            , percentile_disc(0.2::double precision) within group (order by v.speed_avg)        as speed_avg_p20
            , percentile_disc(0.5::double precision) within group (order by v.speed_avg)        as speed_avg_p50
            , percentile_disc(0.8::double precision) within group (order by v.speed_avg)        as speed_avg_p80
            from v1_v2_en_v3_data_15min as v
            where 1=1
            and v.timestamp_rounded >= ((select now() - '1 year'::interval))
            group by 
              v.sensor
            , (date_part('dow'::text, v.timestamp_rounded))
            , (v.timestamp_rounded::time without time zone)
        )

        select
          aq.sensor
        , aq.timestamp_rounded
        , aq.total_count
        , aq.count_down
        , aq.count_up
        , aq.density_avg
        , aq.speed_avg
        , aq.basedonxmessages
        , p.total_count_p10
        , p.total_count_p20
        , p.total_count_p50
        , p.total_count_p80
        , p.total_count_p90
        , p.count_down_p10
        , p.count_down_p20
        , p.count_down_p50
        , p.count_down_p80
        , p.count_down_p90
        , p.count_up_p10
        , p.count_up_p20
        , p.count_up_p50
        , p.count_up_p80
        , p.count_up_p90
        , p.density_avg_p20
        , p.density_avg_p50
        , p.density_avg_p80
        , p.speed_avg_p20
        , p.speed_avg_p50
        , p.speed_avg_p80
        from v1_v2_en_v3_data_15min     as aq
        left join percentiles           as p    on  aq.sensor::text = p.sensor::text
                                                and date_part('dow'::text, aq.timestamp_rounded) = p.dayofweek::double precision 
                                                and aq.timestamp_rounded::time without time zone = p.castedtimestamp
        order by 
          aq.sensor
        , aq.timestamp_rounded
        ;
    """,


    'cmsa_15min_view_v8_realtime_predict': r"""
      CREATE VIEW cmsa_15min_view_v8_realtime_predict AS

        with mat_view_updated as (
            select
            sensor
            , max(timestamp_rounded) + '00:15:00'::interval as last_updated
            from cmsa_15min_view_v8_materialized
            where timestamp_rounded > (now() - '3 days'::interval)
            group by sensor
        )

        , time_serie as (
            select
            mat_view_updated.sensor
            , generate_series(mat_view_updated.last_updated, now() + '01:00:00'::interval, '00:15:00'::interval) as timestamp_rounded
            from mat_view_updated
        )

        , v2_selectie as (
            select
            o.id
            , o.sensor
            , o.timestamp_start
            ,      date_trunc('hour'::text, o.timestamp_start) 
                + (date_part('minute'::text, o.timestamp_start)::integer / 15)::double precision 
                * '00:15:00'::interval                                                              as timestamp_rounded
            , 1                                                                                     as aantal
            from telcameras_v2_observation      as o
            left join mat_view_updated          as u    on o.sensor::text = u.sensor::text
            where (
                o.id in (
                    select
                    t.id
                    from (
                        select
                        id
                        , row_number() over (
                                partition by 
                                sensor
                                , timestamp_start
                                order by
                                sensor
                                , timestamp_start
                                , timestamp_message desc
                        ) as row_num
                        from telcameras_v2_observation
                        where timestamp_start > (now() - '1 days'::interval)
                    ) as t
                    where t.row_num = 1
                )
            )
            and o.timestamp_start > (now() - '1 days'::interval)
            and (
                o.timestamp_start > u.last_updated
                or u.last_updated is null
            )
        )

        , v2_sensor_15min_sel as (
            select
            v2_selectie.sensor
            , v2_selectie.timestamp_rounded
            from v2_selectie
            group by
            v2_selectie.sensor
            , v2_selectie.timestamp_rounded
            order by
            v2_selectie.sensor
            , v2_selectie.timestamp_rounded
        )

        , v2_observatie_snelheid as (
            with v2_observatie_persoon as (
                select
                sel.sensor
                , sel.timestamp_rounded
                , pa.speed
                , string_to_array(substr(pa.geom::text, "position"(pa.geom::text, '('::text) + 1, "position"(pa.geom::text, ')'::text) - "position"(pa.geom::text, '('::text) - 1), ' '::text) as tijd_array
                from telcameras_v2_personaggregate  as pa
                join v2_selectie                    as sel  on  pa.observation_id = sel.id
                                                            and pa.observation_timestamp_start = sel.timestamp_start
                where 1=1
                and pa.observation_timestamp_start > (now() - '1 days'::interval)
                and pa.speed is not null
                and pa.geom is not null
                and pa.geom::text <> ''::text
            
                union all
            
                select
                sel2.sensor
                , sel2.timestamp_rounded
                , pa.speed
                , array['1'::text, '2'::text]   as tijd_array
                from telcameras_v2_personaggregate  as pa
                join v2_selectie                    as sel2     on  pa.observation_id = sel2.id
                                                                and pa.observation_timestamp_start = sel2.timestamp_start
                where 1=1
                and pa.observation_timestamp_start > (now() - '1 days'::interval)
                and pa.speed is not null
                and (
                    pa.geom is null
                    or pa.geom::text = ''::text
                ) 
            )
            
            select
            sensor
            , timestamp_rounded
            , case
                when sum(tijd_array[cardinality(tijd_array)]::numeric - tijd_array[1]::numeric) > 0::numeric
                    then  sum(speed * (tijd_array[cardinality(tijd_array)]::numeric - tijd_array[1]::numeric)::double precision) 
                        / sum(tijd_array[cardinality(tijd_array)]::numeric - tijd_array[1]::numeric)::double precision
                else 0::double precision
            end as speed_avg
            from v2_observatie_persoon
            group by
            sensor
            , timestamp_rounded 
        )

        , v2_countaggregate_zone_count as (
            select
            sel.sensor
            , sel.timestamp_rounded
            , max(c.azimuth)                as azimuth
            , sum(c.count_in)               as count_in
            , sum(c.count_out)              as count_out
            , sum(c.count_in + c.count_out) as total_count
            , avg(c.count)                  as area_count
            , max(c.area)                   as area
            , count(*)                      as basedonxmessages
            from telcameras_v2_countaggregate   as c
            join v2_selectie                    as sel   on  c.observation_id = sel.id
                                                        and c.observation_timestamp_start = sel.timestamp_start
            where 1=1
            and c.observation_timestamp_start > (now() - '1 days'::interval)
            group by
            sel.sensor
            , sel.timestamp_rounded
        )

        , v3_selectie as (
            /* V3 selection, HIG data */    
            /* 
            * Some observation records do have duplicates, for example id 1701379 for sensor CMSA-GAWW-16 (unique key = sensor + timestamp)
            * In these cases the last record (based on the create_at field) is taken, assuming these are better (corrections).
            *
            * Q = What are de exact definitions for the timestamp and created_at fields?
            * A = ...?
            *
            * Q = There are observations for 1 sensor with a different long/lat, for example sensor CMSA-GAWW-14, date 2021-01-06 vs. 2021-01-19?
            * A = In these cases the lat and long were adjusted during the test phase to get better insights.
            * 
            * Q = Why take only data from last year and exclude last 18 minutes?
            * A = ...?
            **/
            select
            o.id            as observation_id
            , o.sensor
            , o.timestamp
            , date_trunc('hour'::text, o.timestamp) + (date_part('minute'::text, o.timestamp)::integer / 15)::double precision * '00:15:00'::interval as timestamp_rounded
            , 1 as aantal
            , density
            from telcameras_v3_observation as o
            left join mat_view_updated          as u    on o.sensor::text = u.sensor::text
            where (
                o.id in (                                       -- If multiple rows are present (based on sensor + timestamp) then pick last one based on latest date in create_at field
                    select
                    t.id
                    from (
                        select
                        id
                        , row_number() over (
                            partition by 
                            sensor
                            , "timestamp"
                            order by   
                            sensor
                            , timestamp
                            , created_at desc
                        ) as row_num
                        from telcameras_v3_observation
                        where timestamp > (now() - '1 days'::interval)
                    ) t
                    where t.row_num = 1
                )
            )
            and o.timestamp > (now() - '1 days'::interval)  -- Retreive only data from for 1 day (based on current timestamp)
            and (                                           -- ..?
                o.timestamp > u.last_updated
                or u.last_updated is null
            )
        )

        , v3_sensor_15min_sel as (
            select 
            sel.sensor                                                    -- name of sensor
            , sel.timestamp_rounded                                         -- the quarter to which this data applies
            , sum(aantal)                       as basedonxobservations     -- number of observations for specifc sensor (should be 15, 1 per minute)
            , sum(grpagg.count)                 as count                    -- number of counted objects (pedestrians/cyclist) within the quarter for specific azimuth (direction)
            , sum(sel.density) / sum(aantal)    as density_avg              -- calculate the average density by summing the density for all observations within the specific quarter and divide this by the count of observations (should be 15, 1 per minute)
            , grpagg.azimuth                                                -- the direction in degrees
            , row_number() over (
                partition by
                sel.sensor
                , sel.timestamp_rounded
                order by 
                grpagg.azimuth
            )                                 as azimuth_seqence_number   -- set ordernumber by azimuth, causing number 1 is always the same azimuth (needed to determine up/down direction)
            , sum(grpagg.cumulative_distance)   as cumulative_distance      -- sum over the cumulative distance in meters for the relevant quarter
            , sum(grpagg.cumulative_time)       as cumulative_time          -- sum over the cumulative time in meters for the relevant quarter
        -- , sum(grpagg.median_speed)          as median_speed             -- sum over the median speed in meters/seconds, not needed because this median_speed is coming from the observation and therefore is per 1 minute
        --                                                                 -- so for a better calculation we use a new calculation with cumulative_distance / cumulative_time
            from telcameras_v3_groupaggregate       as grpagg
            inner join v3_selectie                  as sel      on grpagg.observation_id = sel.observation_id
            where 1=1
            and grpagg.observation_id in (
                select observation_id 
                from v3_selectie
            )
            group by
            sel.sensor
            , sel.timestamp_rounded
            , grpagg.azimuth
            order by 
            sel.sensor
            , sel.timestamp_rounded
        )

        , v3_data as (
            select
            up.sensor                                                     -- name of sensor
            , up.timestamp_rounded                                          -- the quarter to which this data applies
            , up.basedonxobservations                                       -- number of observations for specifc sensor (should be 15, 1 per minute)
            , up.density_avg                                                -- average density over the specific quarter, don't sum the up an down azimuth (directions) because density is coming from the observation table wich doesn't contain azimuth
            , up.count + down.count             as total_count              -- total count (wich contains both azimuth directions)
            , up.cumulative_distance 
                + down.cumulative_distance      as cumulative_distance      -- cumulative distance (wich contains both azimuth directions)
            , up.cumulative_time 
                + down.cumulative_time          as cumulative_time          -- cumulative time (wich contains both azimuth directions)
            , (   up.cumulative_distance 
                + down.cumulative_distance
            )
                /    
            nullif(
                up.cumulative_time 
                + down.cumulative_time
                , 0
            )                                 as speed_avg                -- average speed 
            /* direction 1 */
        -- , up.azimuth                                                    -- the first azimuth, direction in degrees (up)
            , up.count                          as count_up                 -- count for azimuth nr.1 (direction 1)
        -- , up.cumulative_distance                                        -- cumulative distance for azimuth 1, drection in degrees (up)
        -- , up.cumulative_time                                            -- cumulative time for azimuth 1, drection in degrees (up)
        -- , up.median_speed                   as median_speed_up          -- median speed for azimuth 1, drection in degrees (up)
            /* direction 2 */
        --  , down.azimuth                                                 -- the second azimuth, direction in degrees (down)
            , down.count                        as count_down               -- count for azimuth nr.2 (direction 2)
        -- , down.cumulative_distance                                      -- cumulative distance for azimuth 2, drection in degrees (down)
        -- , down.cumulative_time                                          -- cumulative time for azimuth 2, drection in degrees (down)
        -- , down.median_speed                 as median_speed_down        -- median speed for azimuth 1, drection in degrees (up)
            from v3_sensor_15min_sel        as up
            inner join v3_sensor_15min_sel  as down     on  up.sensor = down.sensor
                                                        and up.timestamp_rounded = down.timestamp_rounded
                                                        and down.azimuth_seqence_number = 2
            where 1=1
            and up.azimuth_seqence_number = 1                        
        )

        , aggregatedbyquarter as (
            select
            sel3.sensor
            , sel3.timestamp_rounded
            , case
                when left(sel3.sensor, 4) in ('GADM', 'GAMM')           -- This filter applies to zone sensors for wich only the area_count is filled  
                then coalesce(oc.area_count::integer, 0)
                else coalesce(oc.total_count::integer, 0)
            end                                   as total_count
            , coalesce(oc.count_in::integer, 0)     as count_up
            , coalesce(oc.count_out::integer, 0)    as count_down
            , case
                when    oc.area is not null
                    and oc.area <> 0::double precision
                    and oc.area_count is not null
                    and oc.area_count > 0::numeric 
                        then oc.area_count::double precision / oc.area
                    else null::double precision
            end                                   as density_avg
            , os.speed_avg
            , oc.basedonxmessages
            from v2_sensor_15min_sel                as sel3
            left join v2_observatie_snelheid        as os       on  sel3.sensor::text = os.sensor::text
                                                                and sel3.timestamp_rounded = os.timestamp_rounded
            left join v2_countaggregate_zone_count  as oc       on sel3.sensor::text = oc.sensor::text
                                                                and sel3.timestamp_rounded = oc.timestamp_rounded
            
            union all
            
            select 
            sensor
            , timestamp_rounded
            , total_count
            , count_up
            , count_down
            , density_avg
            , speed_avg
            , basedonxobservations      as basedonxmessages
            from v3_data
        )

        , percentiles as (
            select
            sensor
            , date_part('dow'::text, timestamp_rounded)     as dayofweek
            , timestamp_rounded::time without time zone     as castedtimestamp
            , avg(total_count_p10)                          as total_count_p10
            , avg(total_count_p20)                          as total_count_p20
            , avg(total_count_p50)                          as total_count_p50
            , avg(total_count_p80)                          as total_count_p80
            , avg(total_count_p90)                          as total_count_p90
            , avg(count_down_p10)                           as count_down_p10
            , avg(count_down_p20)                           as count_down_p20
            , avg(count_down_p50)                           as count_down_p50
            , avg(count_down_p80)                           as count_down_p80
            , avg(count_down_p90)                           as count_down_p90
            , avg(count_up_p10)                             as count_up_p10
            , avg(count_up_p20)                             as count_up_p20
            , avg(count_up_p50)                             as count_up_p50
            , avg(count_up_p80)                             as count_up_p80
            , avg(count_up_p90)                             as count_up_p90
            , avg(density_avg_p20)                          as density_avg_p20
            , avg(density_avg_p50)                          as density_avg_p50
            , avg(density_avg_p80)                          as density_avg_p80
            , avg(speed_avg_p20)                            as speed_avg_p20
            , avg(speed_avg_p50)                            as speed_avg_p50
            , avg(speed_avg_p80)                            as speed_avg_p80
            from cmsa_15min_view_v8_materialized
            where timestamp_rounded >= (
                (select now() - '8 days'::interval)
            )
            group by
            sensor
            , (date_part('dow'::text, timestamp_rounded))
            , (timestamp_rounded::time without time zone) 
        )

        , laatste_2_uur_data as (
            select
            sensor
            , timestamp_rounded
            , total_count
            , basedonxmessages
            , bronnr
            from (
                select
                sensor
                , timestamp_rounded
                , total_count * 15 / basedonxmessages   as total_count
                , basedonxmessages
                , rank() over (
                    partition by sensor
                    order by timestamp_rounded desc
                )                                         as bronnr
                from aggregatedbyquarter
                where 1=1
                and timestamp_rounded < (now() - '00:20:00'::interval)
                and timestamp_rounded > (now() - '02:20:00'::interval)
                and basedonxmessages >= 10
            ) as rank_filter
            where bronnr < 9 
        )

        , laatste_2_uur_data_compleet as (
            select
            sensor
            , timestamp_rounded
            , total_count
            , basedonxmessages
            , bronnr
            from laatste_2_uur_data
            where sensor::text in (
                select sensor
                from laatste_2_uur_data
                group by sensor
                having count(*) = 8
            )
        )

        , komende_2_uur_data as (
            select
            sensor
            , timestamp_rounded
            , toepnr
            from (
                select
                sensor
                , timestamp_rounded
                , rank() over (
                    partition by sensor
                    order by timestamp_rounded
                )                     as toepnr
                from time_serie
                where timestamp_rounded > (now() - '00:20:00'::interval)
            ) as rank_filter
            where toepnr < 9
        )

        , alle_data_met_vc as (
            select
            d.sensor
            , d.timestamp_rounded_bron
            , d.total_count
            , d.basedonxmessages
            , d.bronnr
            , d.toepnr
            , vi.intercept_waarde
            , vc.coefficient_waarde
            , d.timestamp_rounded_toep
            from (
                select
                b.sensor
                , b.timestamp_rounded       as timestamp_rounded_bron
                , b.total_count
                , b.basedonxmessages
                , b.bronnr
                , k.toepnr
                , k.timestamp_rounded       as timestamp_rounded_toep
                from laatste_2_uur_data_compleet    as b
                join komende_2_uur_data             as k    on b.sensor::text = k.sensor::text
            )                                                   as d
            left join peoplemeasurement_voorspelintercept       as vi   on  vi.sensor::text = d.sensor::text
                                                                        and vi.toepassings_kwartier_volgnummer = d.toepnr
            left join peoplemeasurement_voorspelcoefficient     as vc   on  vc.sensor::text = d.sensor::text
                                                                        and vc.bron_kwartier_volgnummer = d.bronnr
                                                                        and vc.toepassings_kwartier_volgnummer = d.toepnr
            order by
            d.sensor
            , d.toepnr
            , d.bronnr
        )

        , voorspel_berekening as (
            select
            vc.sensor
            , vc.timestamp_rounded_toep
            , vc.toepnr
            , vc.total_count_voorspeld + vi.intercept_waarde    as total_count_forecast
            from (
                select
                sensor
                , timestamp_rounded_toep
                , toepnr
                , sum(
                    total_count::double precision 
                    * coefficient_waarde
                )                                     as total_count_voorspeld
                from alle_data_met_vc
                group by
                sensor
                , timestamp_rounded_toep
                , toepnr
                order by
                sensor
                , timestamp_rounded_toep
                , toepnr
            )                                               as vc
            left join peoplemeasurement_voorspelintercept   as vi   on  vi.sensor::text = vc.sensor::text
                                                                    and vi.toepassings_kwartier_volgnummer = vc.toepnr 
        )

        select
        s.sensor
        , s.timestamp_rounded
        , coalesce(aq.total_count::numeric, 0::numeric)                     as total_count
        , vb.total_count_forecast
        , coalesce(aq.count_down::numeric,  0::numeric)                     as count_down
        , coalesce(aq.count_up::numeric,    0::numeric)                     as count_up
        , coalesce(aq.density_avg,          0::double precision)            as density_avg
        , coalesce(aq.speed_avg,            0::numeric::double precision)   as speed_avg
        , coalesce(aq.basedonxmessages,     0::bigint)                      as basedonxmessages
        , coalesce(p.total_count_p10,       0::numeric)                     as total_count_p10
        , coalesce(p.total_count_p20,       0::numeric)                     as total_count_p20
        , coalesce(p.total_count_p50,       0::numeric)                     as total_count_p50
        , coalesce(p.total_count_p80,       0::numeric)                     as total_count_p80
        , coalesce(p.total_count_p90,       0::numeric)                     as total_count_p90
        , coalesce(p.count_down_p10,        0::numeric)                     as count_down_p10
        , coalesce(p.count_down_p20,        0::numeric)                     as count_down_p20
        , coalesce(p.count_down_p50,        0::numeric)                     as count_down_p50
        , coalesce(p.count_down_p80,        0::numeric)                     as count_down_p80
        , coalesce(p.count_down_p90,        0::numeric)                     as count_down_p90
        , coalesce(p.count_up_p10,          0::numeric)                     as count_up_p10
        , coalesce(p.count_up_p20,          0::numeric)                     as count_up_p20
        , coalesce(p.count_up_p50,          0::numeric)                     as count_up_p50
        , coalesce(p.count_up_p80,          0::numeric)                     as count_up_p80
        , coalesce(p.count_up_p90,          0::numeric)                     as count_up_p90
        , coalesce(p.density_avg_p20,       0::double precision)            as density_avg_p20
        , coalesce(p.density_avg_p50,       0::double precision)            as density_avg_p50
        , coalesce(p.density_avg_p80,       0::double precision)            as density_avg_p80
        , coalesce(p.speed_avg_p20,         0::numeric::double precision)   as speed_avg_p20
        , coalesce(p.speed_avg_p50,         0::numeric::double precision)   as speed_avg_p50
        , coalesce(p.speed_avg_p80,         0::numeric::double precision)   as speed_avg_p80
        from time_serie                 as s
        left join aggregatedbyquarter   as aq   on  s.sensor::text = aq.sensor::text
                                                and aq.timestamp_rounded = s.timestamp_rounded
        left join percentiles           as p    on  aq.sensor::text = p.sensor::text
                                                and date_part('dow'::text, aq.timestamp_rounded) = p.dayofweek
                                                and aq.timestamp_rounded::time without time zone = p.castedtimestamp
        left join voorspel_berekening   as vb   on  vb.sensor::text = s.sensor::text
                                                and s.timestamp_rounded = vb.timestamp_rounded_toep
        order by
        s.sensor
        , s.timestamp_rounded
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
