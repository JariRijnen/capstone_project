class DropTables:
    drop_staging_weather = """DROP TABLE IF EXISTS staging_weather CASCADE"""
    drop_staging_wildfires = """DROP TABLE IF EXISTS staging_wildfires CASCADE"""
    drop_weather_measurements = """DROP TABLE IF EXISTS weather_measurements CASCADE"""
    drop_wildfires = """DROP TABLE IF EXISTS wildfires CASCADE"""
    drop_time_table = """DROP TABLE IF EXISTS time_table CASCADE"""
    drop_weather_stations = """DROP TABLE IF EXISTS weather_stations CASCADE"""
    drop_weather_measurements = """DROP TABLE IF EXISTS weather_measurements CASCADE"""
    drop_date_table = """DROP TABLE IF EXISTS date_table CASCADE"""
    drop_us_state = """DROP TABLE IF EXISTS us_state CASCADE"""
    drop_distance_table = """DROP TABLE IF EXISTS distance_table CASCADE"""

    drop_tables = [drop_staging_weather, drop_staging_wildfires, drop_weather_measurements,
                   drop_wildfires, drop_distance_table, drop_time_table, drop_weather_stations]
