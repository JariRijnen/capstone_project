class InsertTables:
    weather_measurements_insert = """
        INSERT INTO weather_measurements
        SELECT DISTINCT
                sw.index,
                sw.station_id,
                sw.date,
                sw.awnd,
                sw.evap,
                sw.fmtm,
                sw.pgtm,
                sw.prcp,
                sw.sn52,
                sw.sn53,
                sw.sn55,
                sw.snow,
                sw.snwd,
                sw.sx02,
                sw.sx52,
                sw.sx53,
                sw.sx55,
                sw.tavg,
                sw.tmax,
                sw.tmin,
                sw.tobs,
                sw.wdf1,
                sw.wdf2,
                sw.wdf5,
                sw.wdmv,
                sw.wsf1,
                sw.wsf2,
                sw.wsf5,
                sw.wsfg
        FROM staging_weather sw;
    """

    weather_stations_insert = """
        INSERT INTO weather_stations
        WITH station_table AS (
            SELECT DISTINCT station_id,
                            station_name,
                            latitude,
                            longitude,
                            elevation
            FROM staging_weather)
        SELECT
                st.station_id,
                st.station_name,
                SUBSTRING(split_part(st.station_name, ', ', 2), 1, 2),
                st.latitude,
                st.longitude,
                ST_SetSRID(ST_POINT(st.longitude, st.latitude), 4326),
                st.elevation
        FROM station_table st;
    """

    date_table_insert = """
        INSERT INTO date_table
        WITH date_table1 AS (
            SELECT date
            FROM staging_weather
            UNION
            SELECT to_date(floor(discovery_date)::text, 'J')
            FROM staging_wildfires
            UNION
            SELECT (to_date(floor(cont_date)::text, 'J'))
            FROM staging_wildfires)
        SELECT DISTINCT
                dt.date,
                DATE_PART(doy, dt.date),
                DATE_PART(m, dt.date),
                DATE_PART(dow, dt.date),
                DATE_PART(w, dt.date),
                DATE_PART(y, dt.date)
        FROM date_table1 dt;
    """

    time_table_insert = """
        INSERT INTO time_table
        WITH cte_time AS (
            SELECT DECODE(discovery_time, '', NULL, discovery_time)::time as fire_time
            FROM staging_wildfires
            UNION
            SELECT DECODE(cont_time, '', NULL, cont_time)::time as fire_time
            FROM staging_wildfires)
        SELECT DISTINCT
                fire_time::time,
                extract(hour from fire_time::time) as hour,
                extract(minute from fire_time::time) as minute
        FROM cte_time;
    """

    us_state_insert = """
        INSERT INTO us_state
        SELECT DISTINCT us_state
        FROM staging_wildfires
    """

    wildfires_insert = """
        INSERT INTO wildfires
        SELECT
                wildfire_id,
                fod_id,
                source_system,
                nwcg_reporting_agency,
                nwcg_reporting_unit_name,
                source_reporting_unit_name,
                local_fire_report_id,
                local_incident_id,
                fire_code,
                fire_name,
                ics_209_incident_number,
                ics_209_name,
                complex_name,
                to_date(floor(discovery_date)::text, 'J'),
                DECODE(discovery_time, '', NULL, discovery_time)::time,
                stat_cause_descr,
                (to_date(floor(cont_date)::text, 'J')),
                DECODE(cont_time, '', NULL, cont_time)::time,
                fire_size,
                fire_size_class,
                latitude,
                longitude,
                ST_SetSRID(ST_POINT(longitude, latitude), 4326),
                owner_descr,
                us_state,
                us_county
        FROM staging_wildfires;
    """

    insert_dimension_tables = [weather_measurements_insert, wildfires_insert]
    insert_fact_tables = [weather_stations_insert, date_table_insert,
                          time_table_insert, us_state_insert]
