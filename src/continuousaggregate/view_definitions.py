VIEW_STRINGS = {
    # NOTE: the regex in these queries cause a DeprecationWarning: invalid escape sequence
    # For this reason we use a rawstring for these queries
    'vw_cmsa_15min_v01_aggregate': r"""
      CREATE VIEW vw_cmsa_15min_v01_aggregate AS
        WITH period_of_time AS (
            select
              case
                when max(timestamp_rounded) is null then (current_date - '1 year'::interval)::date
                when max(timestamp_rounded)::date < (current_date - '5 days'::interval)::date then max(timestamp_rounded)::date
                else (max(timestamp_rounded) - '1 day'::interval)::date
              end as start_date
            , case
                when max(timestamp_rounded) is null then (current_date - '1 year'::interval + '2 month'::interval)::date
                when (max(timestamp_rounded) + '2 month'::interval)::date < current_date then (max(timestamp_rounded) + '2 month'::interval)::date
                else current_date
              end as end_date
            from continuousaggregate_cmsa15min
        )
        , v2_zone_sensor as (
            -- Zone sensors give 2 count values (one per area) but in the observation table there is only 1 sensorname. This piece of code generates a new sensorname which contains the area because we want to know the count value per area.
            -- Filter just 1 day from performance perspective. When filtering less (for example 1 hour) it is possible that there is no data available. Data is only available when there is actually a participant in the images of the sensor.
            select 
              external_id
            , substring(external_id, 0, length(external_id) -5) as sensor
            from telcameras_v2_countaggregate
            where 1=1
            and left(external_id, 4) in ('GADM', 'GAMM')
            and observation_timestamp_start::date > (select end_date from period_of_time)
            group by external_id
        )
        , v2_selectie as (
            select
              o.id
            , coalesce(zs.external_id, o.sensor) as sensor          -- use external_id for zone sensors (these contain the area)
            , o.timestamp_start
            ,       date_trunc('hour'::text, o.timestamp_start) 
                + (date_part('minute'::text, o.timestamp_start)::integer / 15)::double precision 
                * '00:15:00'::interval                                                              as timestamp_rounded
            , 1                                                                                     as aantal
            from telcameras_v2_observation  as o
            left join v2_zone_sensor        as zs   on  left(o.sensor, 4) in ('GADM', 'GAMM')
                                                    and o.sensor = zs.sensor
            where 1=1
            and o.timestamp_start::date >= (select start_date 	from period_of_time)
            and o.timestamp_start::date <= (select end_date		from period_of_time) 
            and o.id in (
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
                    where 1=1
                    and timestamp_start::date >= (select start_date	from period_of_time) 
                    and timestamp_start::date <= (select end_date	from period_of_time)
                ) as t
                where t.row_num = 1
            )
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
        , v2_countaggregate_zone_count as (
            -- For non-zone sensors 
            select
              sel.sensor
            , sel.timestamp_rounded
            , max(c.azimuth)                    as azimuth          -- azimuth is always the same so max()/min() doesn't do anything (just for grouping)
            , sum(c.count_in_scrambled)         as count_in
            , sum(c.count_out_scrambled)        as count_out
            , sum(
                c.count_in_scrambled 
                + c.count_out_scrambled)        as total_count
            , avg(c.count_scrambled)            as area_count
            , max(c.area)                       as area
            from telcameras_v2_countaggregate       as c
            join v2_selectie                        as sel  on  c.observation_id = sel.id
                                                            and c.observation_timestamp_start = sel.timestamp_start
            where 1=1
            and left(sel.sensor, 4) not in ('GADM', 'GAMM')
            group by
            sel.sensor
            , sel.timestamp_rounded
            
            union all
            
            -- For zone sensors (beginning with 'GADM', 'GAMM') use a extra join argument on external_id to get the correct count values. Needed because one observation (observation_id) consist both area count values. 
            select
              sel.sensor
            , sel.timestamp_rounded
            , max(c.azimuth)                    as azimuth
            , sum(c.count_in_scrambled)         as count_in
            , sum(c.count_out_scrambled)        as count_out
            , sum(
                c.count_in_scrambled 
                + c.count_out_scrambled)        as total_count
            , avg(c.count_scrambled)            as area_count
            , max(c.area)                       as area
            from telcameras_v2_countaggregate       as c
            join v2_selectie                        as sel  on  c.observation_id = sel.id
                                                            and c.observation_timestamp_start = sel.timestamp_start
                                                            and c.external_id = sel.sensor
            where 1=1
            and left(sel.sensor, 4) in ('GADM', 'GAMM')
            group by
              sel.sensor
            , sel.timestamp_rounded
        )
        , v2_data as (
            select
              sel3.sensor
            , sel3.timestamp_rounded
            , case
                when left(replace(sel3.sensor, 'CMSA-', ''), 4) in ('GADM', 'GAMM', 'GAAB', 'GABW', 'GACT')         -- This filter applies to zone sensors for wich only the area_count is filled
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
            , sel3.basedonxmessages
            from v2_sensor_15min_sel                as sel3
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
            where 1=1
            and o.timestamp::date >= (select start_date	from period_of_time) 
            and o.timestamp::date <= (select end_date	from period_of_time)
            and (
                o.id in (                             -- If multiple rows are present (based on sensor + timestamp) then pick last one based on latest date in create_at field
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
                        where 1=1
                        and timestamp::date >= (select start_date	from period_of_time)
                        and timestamp::date <= (select end_date		from period_of_time)
                    ) t
                    where t.row_num = 1
                )
            )

        )
        , v3_sensor_15min_sel as (
            select 
              sel.sensor                                                    -- name of sensor
            , sel.timestamp_rounded                                         -- the quarter to which this data applies
            , sum(aantal)                       as basedonxobservations     -- number of observations for specifc sensor (should be 15, 1 per minute)
            , sum(grpagg.count_scrambled)       as count                    -- number of counted (scrambled) objects (pedestrians/cyclist) within the quarter for specific azimuth (direction)
            , sum(sel.density) / sum(aantal)    as density_avg              -- calculate the average density by summing the density for all observations within the specific quarter and divide this by the count of observations (should be 15, 1 per minute)
            , grpagg.azimuth                                                -- the direction in degrees
            , row_number() over (
                partition by
                sel.sensor
                , sel.timestamp_rounded
                order by 
                grpagg.azimuth
            )                                   as azimuth_seqence_number   -- set ordernumber by azimuth, causing number 1 is always the same azimuth (needed to determine up/down direction)
            , sum(grpagg.cumulative_distance)   as cumulative_distance      -- sum over the cumulative distance in meters for the relevant quarter
            , sum(grpagg.cumulative_time)       as cumulative_time          -- sum over the cumulative time in meters for the relevant quarter
            -- , sum(grpagg.median_speed)       as median_speed             -- sum over the median speed in meters/seconds, not needed because this median_speed is coming from the observation and therefore is per 1 minute
            --                                                              -- so for a better calculation we use a new calculation with cumulative_distance / cumulative_time
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
            )                                   as speed_avg                -- average speed 
            /* direction 1 */
            -- , up.azimuth                                                 -- the first azimuth, direction in degrees (up)
            , up.count                          as count_up                 -- count for azimuth nr.1 (direction 1)
            -- , up.cumulative_distance                                     -- cumulative distance for azimuth 1, drection in degrees (up)
            -- , up.cumulative_time                                         -- cumulative time for azimuth 1, drection in degrees (up)
            -- , up.median_speed                as median_speed_up          -- median speed for azimuth 1, drection in degrees (up)
            /* direction 2 */
            --  , down.azimuth                                              -- the second azimuth, direction in degrees (down)
            , down.count                        as count_down               -- count for azimuth nr.2 (direction 2)
            -- , down.cumulative_distance                                   -- cumulative distance for azimuth 2, drection in degrees (down)
            -- , down.cumulative_time                                       -- cumulative time for azimuth 2, drection in degrees (down)
            -- , down.median_speed                 as median_speed_down     -- median speed for azimuth 1, drection in degrees (up)
            from v3_sensor_15min_sel        as up
            inner join v3_sensor_15min_sel  as down     on  up.sensor = down.sensor
                                                        and up.timestamp_rounded = down.timestamp_rounded
                                                        and down.azimuth_seqence_number = 2
            where 1=1
            and up.azimuth_seqence_number = 1                        
        )
        , v1_v2_en_v3_data_15min as (
            select 
              sensor
            , timestamp_rounded
            , total_count
            , count_down
            , count_up
            , density_avg
            , basedonxmessages
            from peoplemeasurement_v1_data
            
            union all
            
            select 
              sensor
            , timestamp_rounded
            , total_count
            , count_down
            , count_up
            , density_avg
            , basedonxmessages
            from v2_data
            
            union all
            
            select 
              sensor
            , timestamp_rounded
            , total_count
            , count_down
            , count_up
            , density_avg
            , basedonxobservations    as basedonxmessages
            from v3_data
        )
        , percentiles as (
            select
              sensor
            , date_part('dow'::text, timestamp_rounded)::integer                              as dayofweek
            , timestamp_rounded::time without time zone                                       as castedtimestamp
            , percentile_disc(0.1::double precision) within group (order by total_count)      as total_count_p10
            , percentile_disc(0.2::double precision) within group (order by total_count)      as total_count_p20
            , percentile_disc(0.5::double precision) within group (order by total_count)      as total_count_p50
            , percentile_disc(0.8::double precision) within group (order by total_count)      as total_count_p80
            , percentile_disc(0.9::double precision) within group (order by total_count)      as total_count_p90
            , percentile_disc(0.1::double precision) within group (order by count_down)       as count_down_p10
            , percentile_disc(0.2::double precision) within group (order by count_down)       as count_down_p20
            , percentile_disc(0.5::double precision) within group (order by count_down)       as count_down_p50
            , percentile_disc(0.8::double precision) within group (order by count_down)       as count_down_p80
            , percentile_disc(0.9::double precision) within group (order by count_down)       as count_down_p90
            , percentile_disc(0.1::double precision) within group (order by count_up)         as count_up_p10
            , percentile_disc(0.2::double precision) within group (order by count_up)         as count_up_p20
            , percentile_disc(0.5::double precision) within group (order by count_up)         as count_up_p50
            , percentile_disc(0.8::double precision) within group (order by count_up)         as count_up_p80
            , percentile_disc(0.9::double precision) within group (order by count_up)         as count_up_p90
            , percentile_disc(0.2::double precision) within group (order by density_avg)      as density_avg_p20
            , percentile_disc(0.5::double precision) within group (order by density_avg)      as density_avg_p50
            , percentile_disc(0.8::double precision) within group (order by density_avg)      as density_avg_p80
            from continuousaggregate_cmsa15min
            where 1=1
            and timestamp_rounded >= ((select now() - '1 year'::interval))
            group by 
              sensor
            , (date_part('dow'::text, timestamp_rounded))
            , (timestamp_rounded::time without time zone)
        )
        
        select
          concat(aq.sensor, '~', aq.timestamp_rounded)  as bk_continuousaggregate_cmsa_15_min
        , aq.sensor
        , aq.timestamp_rounded
        , aq.total_count
        , aq.count_down
        , aq.count_up
        , aq.density_avg
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
        from v1_v2_en_v3_data_15min     as aq
        left join percentiles           as p    on  aq.sensor::text = p.sensor::text
                                                and date_part('dow'::text, aq.timestamp_rounded) = p.dayofweek::double precision 
                                                and aq.timestamp_rounded::time without time zone = p.castedtimestamp
        order by 
          aq.sensor
        , aq.timestamp_rounded
        ;
    """,

    'vw_cmsa_15min_v01_predict': r"""
      CREATE VIEW vw_cmsa_15min_v01_predict AS
        
        /*
        * Tijd selecteren die overal gebruikt gaat worden.
        * Dit wordt hier gedaan zodat voor testen deze makkelijk aangepast kan worden naar het gewenste tijdstip
        */
        with use_date as (
            select 
                now() - interval '00:02:30' as use_date -- Let op dat dit inteval korter is dan de wachttijd die gebruikt wordt na het afsluiten van het kwartier
        )
        
        , use_date_kw as (
            select
                date_trunc('hour', use_date) + interval  '15 minute' * floor(extract(minute from (use_date))/15)  as use_date_kw_0
            from use_date
        )
        
        /*
        * Lijst met alle tijdstippen die nodig zijn en hun bijbehorende volgnummer (1 t/m 21).
        * De eerste twaalf kwartieren (1 t/m 12) zijn voor de berekening van de ophoog factor
        * De kwartier 13 t/m 20 zijn voor de voorspelling
        * De kwartier 12 t/m 21 zijn voor "vloeiende curve". Waarbij voor 12 en 21 geen curve wordt berkent. Deze worden alleen in de berekening gebruikt
        */
        , date_serie_prediction as (
            select
              *
            , extract(ISODOW from use_date_kw) as weekdag
            , use_date_kw::time as tijd
            from (
                select
                generate_series(
                    use_date_kw_0 - '03:00:00'::interval --Uren er voor voor berekening ophoog factor.
                , use_date_kw_0 + '02:00:00'::interval --'01:45:00' is laatste (8st) kwartier wat voorspeld word, '02:00:00' 9de kwarier wordt gebruikt voor het uitrekken van de "vloeiende curve"
                , '00:15:00'::interval --Stappen van een kwartier    
                ) as use_date_kw
                , generate_series(1,21) as order_nr
                from use_date_kw
            ) as basis_tijd_list
        )
        
        /*
        * Maken van lijst met alle active sensoren
        * Een sensor is actief als hij minimiaal één telling heeft die groter dan 0 is in de afgelopen 2 weken. Hiervoor wordt gekeken naar de 15 minuten view
        */
        , sensor_list as (
            select sensor
            from use_date
            left join continuousaggregate_cmsa15min	as ts on (
                    ts.timestamp_rounded > use_date.use_date - '2 weeks'::interval    	
                and ts.total_count > 0)
            where 1=1
            and sensor not like 'GVCV%'
            group by sensor
        )
        
        /*
        * Combinieren van active sensoren met de gewenste tijden zodat aan elke active sensor alle gewenste tijden gekoppeld worden.
        * Dit voorkomt dat er later gaten vallen als er in de tellingen gaten zitten.
        */
        , date_serie_prediction_sensor as (
            select *
            from sensor_list 
            full join date_serie_prediction on (true)
        )
        
        /*
        * Voor alle sensoren voor alle gewenste tijdstippen de historische data ophalen
        * Hiervoor wordt voor elk tijdstip 8 weken terug gekeken en pakt pakt voor de afgelopen weken het zelfde tijdstip op de zelfde dag van de week.
        * Indien beschikbaar worden er dus voor elk sensor, voor elk tijdstip 8 tellingen ophaalt. Indien er minder beschikbaar zijn worden er minder opgehaald.
        */
        , prediction_historical_curve as (
            select
              dsp.use_date_kw
            , dsp.sensor
            , dsp.order_nr
            , percentile_cont(0.5) within group (order by coalesce(total_count, 0)) as count_mediaan -- Mediaan van de 8 (of minder als niet beschikbaar) waarde voor het betreffende tijdstip en weekdag nemen.
            from date_serie_prediction_sensor 					            as dsp
            left join continuousaggregate_cmsa15min	as ts 	on (
                        dsp.sensor = ts.sensor
                    and	extract(ISODOW from ts.timestamp_rounded) = dsp.weekdag								              -- Juiste dag van de week koppelen
                    and ts.timestamp_rounded::time = dsp.tijd												                        -- Juiste tijd koppelen
                    and	dsp.use_date_kw - '8 weeks'::interval - '2 days'::interval < ts.timestamp_rounded  	-- Zorgen date data van de afgelopen 8 weken wordt mee genomen,  
                    and ts.timestamp_rounded <  dsp.use_date_kw - '2 days'::interval						            -- maar niet de dag waarvoor de voorspelling gemaakt wordt of de voorgaande dag
                    --and (dsp.sensor not in ('CMSA-GAWW-15', 'CMSA-GAWW-16') or ts.timestamp_rounded > '2021-04-09 00:00:00') --resetten van de sensor in not in list vanaf gegeven datum. Dit kan handig zijn bij grote veranderingen
                    --and (dsp.sensor not in ('CMSA-GAWW-17', 'CMSA-GAWW-19') or ts.timestamp_rounded > '2021-04-02 00:00:00') --resetten van de sensor in not in list vanaf gegeven datum. Dit kan handig zijn bij grote veranderingen
            )
            group by
              use_date_kw
            , order_nr
            , dsp.sensor 
        )
        
        /*
        * Op basis van de voorgaande mediaan een mooie voloeiende curve maken door: 
        * Waarde voorgaande kwartier x 0.25, waarde kwartier zelf x 0.5, waarde volgende kwartier x 0.25
        * Maakt de voorspelling niet beter (of slechter) maar oogt wel beter
        */
        , prediction_historical_curve_ruff_smooth as (
            select
              use_date_kw
            , sensor
            , order_nr		
            , count_mediaan
            ,     0.5 * count_mediaan 
                + 0.25 * (LAG(count_mediaan,1,null) OVER (PARTITION by sensor order by order_nr) + LEAD(count_mediaan,1,null) OVER (PARTITION by sensor order by order_nr)) as count_mediaan_glad	--maken glade median op basis van 0.25 voorgaan en volgende. 0.5 * huidige
            from prediction_historical_curve
        )
            
        /*
        * real time en historisch data combineren
        * Alleen als zowel een mediaan als een telling voor de betreffende sensor en kwartier bepaald kan worden wordt er een ophoog factor berekend. Zo niet wordt deze op 1 gezet.
        * Als er een ophoog factor groter dan 4 wordt gevonden wordt deze op 4 gezet. Het gaat dan waarschijnlijk om een foute meting of een uitschieter en op deze manier werkt deze niet te lang door
        */
        , curve_and_realtime as (
            select
              time_curve.sensor
            , time_curve.use_date_kw
            , time_curve.order_nr		
            , time_curve.count_mediaan
            , time_curve.count_mediaan_glad
            , case
                when time_curve.count_mediaan > 0 and rt.total_count > 0  then -- als of de mediaan of de total count niet bestaat is de ophoogfactor 1
                    case
                        when rt.total_count/time_curve.count_mediaan > 4 then 4			
                        else rt.total_count/time_curve.count_mediaan
                    end
                else 1		--maak de ophoogfactor 1 de total_count null is																
              end as ophoog_fact_kw
            , rt.total_count
            from prediction_historical_curve_ruff_smooth				as time_curve
            left join continuousaggregate_cmsa15min	  as rt			      on  time_curve.sensor = rt.sensor
                                                                                and time_curve.use_date_kw = rt.timestamp_rounded
            order by
              time_curve.sensor
            , time_curve.use_date_kw
        )
        
        /*
        * Bepalen ophoogfactor voor de gehele curve.
        * Deze is 0.69 de ophoog factor die voor de voorgaande voorspelling (kwartier geleden) is gemaakt en 0.31 de ophoog factor die voor de huidige voorspelling wordt gemaakt. Ophoog = 0.31 x Ophoog(nieuw) + 0.69 x Ophoog(oud)
        * De waardes zijn zo gekozen dat de ophoog factor van de voorspelling van een half uur geleden nog voor 0.5 mee weegt en de ophoog factor van een uur geleden voor 0.25
        * Omdat het systeem stateles is worden de ophoog vactoren elk kwartier opnieuw berekend op basis van de afgelopen 3 uur. Hierbij wordt als "start waarde" de ophoog factor van het eerste kwartier genomen.
        * Dit lever voor de voorgaande 12 kwartier de volgende gewichten op voor de ophoogfactor voor elk van deze kwartieren.
        */
        , ophoogfactor_kw_frac as (
            select
            *
            , case
                when order_nr = 1	then 0.008 * ophoog_fact_kw 
                when order_nr = 2	then 0.008 * ophoog_fact_kw
                when order_nr = 3	then 0.012 * ophoog_fact_kw
                when order_nr = 4	then 0.017 * ophoog_fact_kw
                when order_nr = 5	then 0.024 * ophoog_fact_kw
                when order_nr = 6	then 0.034 * ophoog_fact_kw
                when order_nr = 7	then 0.048 * ophoog_fact_kw
                when order_nr = 8	then 0.071 * ophoog_fact_kw
                when order_nr = 9	then 0.103 * ophoog_fact_kw
                when order_nr = 10	then 0.149 * ophoog_fact_kw
                when order_nr = 11	then 0.215 * ophoog_fact_kw
                when order_nr = 12	then 0.311 * ophoog_fact_kw
            end as ophoogfactor_kw_frac		
            from curve_and_realtime
        )
        
        /*
        * Berekening ophoog factor voor de sensor voor de voorspelling voor dit kwartier.
        */
        , ophoogfactor_sensor as (
            select
              sensor
            , sum(ophoogfactor_kw_frac)		as ophoogfactor 
            from ophoogfactor_kw_frac
            group by sensor
        )
        
        /*
        * Per sensor de te voorspellen curve uitrekenen
        * Variable de juist naam geven
        */
        select
          op_sen.sensor									as sensor
        , curve.use_date_kw								as timestamp_rounded
        , round(count_mediaan_glad * ophoogfactor)		as prediction
        from ophoogfactor_sensor		as op_sen 
        left join curve_and_realtime	as curve 	on op_sen.sensor = curve.sensor
        where 1=1
        and 12 < order_nr 
        and order_nr < 21
        order by 
          sensor
        , timestamp_rounded
        ;
    """,

    'vw_cmsa_15min_v01_realtime_predict': r"""
      CREATE VIEW vw_cmsa_15min_v01_realtime_predict AS

        select
          rt.sensor
        , rt.timestamp_rounded
        , case
            when pdt.prediction is not null then pdt.prediction
            else rt.total_count
          end                     as total_count
        , pdt.prediction          as total_count_forecast
        , rt.count_down
        , rt.count_up
        , rt.density_avg
        , rt.basedonxmessages
        , rt.total_count_p10
        , rt.total_count_p20
        , rt.total_count_p50
        , rt.total_count_p80
        , rt.total_count_p90
        , rt.count_down_p10
        , rt.count_down_p20
        , rt.count_down_p50
        , rt.count_down_p80
        , rt.count_down_p90
        , rt.count_up_p10
        , rt.count_up_p20
        , rt.count_up_p50
        , rt.count_up_p80
        , rt.count_up_p90
        , rt.density_avg_p20
        , rt.density_avg_p50
        , rt.density_avg_p80
        from continuousaggregate_cmsa15min    as rt 
        left join vw_cmsa_15min_v01_predict			       as pdt   on  rt.sensor = pdt.sensor
                                                                and rt.timestamp_rounded = pdt.timestamp_rounded
                                                                and pdt.timestamp_rounded >= (now() - '00:18:00'::interval)
      ;
    """,
    
}


class WrongIndexException(Exception):
    pass


def get_view_strings(view_strings, view_name, indexes=None):
    """
    Creates query strings for views and it's indexes

    :param view_name:
    :param indexes: a list of tuples containing the columns for indexes to be added.
        Example: indexes=[('sensor', 'timestamp_rounded'), ('timestamp')]
    :return:
    """

    reverse_sql = f"DROP VIEW IF EXISTS {view_name};"

    return {
        'sql': view_strings[view_name],
        'reverse_sql': reverse_sql
    }
