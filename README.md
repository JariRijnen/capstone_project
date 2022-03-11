## Capstone Project Wild Fires

## Purpose
The purpose of this project is to investigate the relations between weather and wildfires in the United States by performing queries on the final Redshift database. E.g. one could investigate how much the rainfall in a previous month relates to the occurrence of wildfires, or could investigate how much the temperature influences the spread (size) of wildfires.

This project consists of an ETL pipeline, which is orchestrated by Airflow (locally) and turns data from S3 storage to a Redshift data model. 

### Other scenarios
**The data was increased by 100x**

The data pipeline is currently run on with a local airflow. By moving this to the cloud, a bigger load of data can be handled. Redshift itself is already cloud-based, and therefore would have no problems with a 100x of the data.

**The pipelines would be run on a daily basis by 7 am every day.**

This could be achieved by setting the Airflow scheduler to a daily schedule started at 7 am.

**The database needed to be accessed by 100+ people.**

The database is currently inside a Redshift cluster with the dc2.large single node type, with fixed local SSD storage. By upgrading this to a ra3 node, the cluster maintains high performance but now with scalable managed storage.

## File Tree

```
README.md
dags
   |-- wildfire_dag.py
definitions.csv
docker-compose.yaml
images
   |-- ER-diagram.png
   |-- airflow_pipeline.png
plugins
   |-- helpers
   |   |-- __init__.py
   |   |-- sql_queries
   |   |   |-- drop_tables.py
   |   |   |-- insert_tables.py
   |-- hooks
   |   |-- redshift_cluster.py
   |-- operators
   |   |-- __init__.py
   |   |-- data_quality.py
   |   |-- drop_tables.py
   |   |-- insert_tables.py
   |   |-- redshift_cluster.py
   |   |-- stage_to_redshift.py
requirements.txt
```

## Data

```
wildfires.parquet 
```

Wildfires data from [Kaggle](https://www.kaggle.com/rtatman/188-million-us-wildfires). Original is a table in a SQLite database. I saved it to .parquet and saved it on S3 in AWS.

It consists of 1.88 million wildfires that occurred in the US from 1992 to 2015.

```
US_weather_all.csv
```

Weather data from [Kaggle](https://www.kaggle.com/cavfiumella/us-weather-daily-summaries-1991-2016). Original is 1001 different csv files. I have merged them into one .csv file and saved that on S3 in AWS.

It consists of daily weather summaries over 450 weather stations across the US from 1991 to 2016. In total there are 4,169,412 daily measurements. Not all stations measure the same statistics. Empty columns were excluded.

## Airflow Data Pipeline

Run locally in Docker container. To start, make sure that Docker is running and execute 

```
docker-compose up
```

![airflow-pipeline](/images/airflow_pipeline.png)

The pipeline is locally executed via Airflow. The following steps are undertaken:
* Begin_execution: a dummy operator to start the pipeline.
* Resume_redshift_cluster: operator that resumes (if necessary) a Redshift cluster in AWS.
* Drop_tables_if_exists: operator that drops redshift tables if exists.
* create_staging_tables_redshift: operator to create the redshift tables needed for staging the data. 
* stage_weather & stage_wildfire: two staging redshift queries that load the data from S3 into the staging tables.
* insert_dimension_tables: operator to insert data from the staging tables into the dimension tables.
* insert_fact_tables_redshift: operator to insert redshift data from the staging tables into the fact tabes.
* insert_distance_table_redshift: operator to insert distance_table from existing dimension and fact tables into the distance table.
* data_quality check: operator with two data quality checks for all of the final fact and dimension tables. 
* pause_redshift_cluster: operator to pause the used redshift cluster.
* stop_execution: dummy operator to close the pipeline.

## ER diagram

![ER-diagram](/images/ER-diagram.png)

The ER-diagram consists of two facts tables, wildfires and weather_measurements. There are five additional dimension tables: us_state, weather_stations, date_table, time_table, distance_table.

There are three options to match wildfires with relevant weather stations. 
* The first is to look at the weather stations in the same state as the wildfire, which only requries a simple join. 
* The second is to by looking at the geographical locations; either by the coordinates directly or by the geom variables. This allows filtering by longitude and latitude, or to search for the closest weather station by MIN(ST_DistanceSphere(weather_stations.geom, wildfires.geom)).
* The third is by joining on the distance_table, which allows for including all weather stations within a certain distance of the wildfire.

## Example Queries
```
SELECT COUNT(DISTINCT wf.wildfire_id), wf.fire_size_class, ROUND(AVG(wm.tavg), 2) AS temp, ROUND(AVG(wm.awnd), 2) AS wind, ROUND(AVG(wm.snow), 2) AS snow, ROUND(AVG(wm.prcp), 2) AS rain
FROM wildfires wf
JOIN weather_stations ws
ON wf.us_state = ws.us_state
JOIN weather_measurements wm
ON wm.station_id = ws.station_id
WHERE wf.discovery_date = wm.date
GROUP BY fire_size_class
ORDER BY fire_size_class;
```
| count| 	fire_size_class| 	temp| 	wind| 	snow| 	rain| 
|-------|----|-----|-----|-----|-----|
| 665170	| A| 	18.29| 	3.4| 	0.33| 	0.91| 
| 921727	| B| 	17.32| 	3.53| 	0.17| 	0.89| 
| 217533	| C| 	17.27| 	3.6| 	0.12| 	0.9| 
| 28278	| D| 	18.36| 	3.77| 	0.12| 	0.81| 
| 14060	| E| 	19.28| 	3.82| 	0.12| 	0.81| 
| 7777	| F| 	20.12| 	3.76| 	0.09| 	0.74| 
| 3773	| G| 	19.44| 	3.5| 	0.04| 	0.71| 

And

```
SELECT ROUND(AVG(wm.tavg), 2) AS temp, ROUND(AVG(wm.awnd), 2) AS wind, ROUND(AVG(wm.snow), 2) AS snow, ROUND(AVG(wm.prcp), 2) AS rain
FROM weather_measurements wm
```

|temp|	wind|	snow|	rain|
|-----|-----|-----|-----|
|12.89|	3.59|	2.34|	2.34|

These two queries show that wildfires on average occur on hotter days, which makes sense, and that larger fires (higher fire size class) generally have a lower rain and snowfall in that state. Because weather data is averaged over all weather stations within a state, it is possible to register snowfall and a wildfire at the same at.

See [result_queries.ipynb](https://github.com/JariRijnen/capstone_project/result_queries.ipynb) for more examples.

## Data Dictonary

**Wildfires**

| Column         | Description     |
|--------------|-----------|
| wildfire_id | Unique identifier      |
| fod_id      |Global unique identifier |
| source_system       | Name of or other identifier for source database or system that the record was drawn from|
|  nwcg_reporting_agency     |Active National Wildlife Coordinating Group (NWCG) Unit Identifier for the agency preparing the fire report |
| nwcg_reporting_unit_name      | Active NWCG Unit Name for the unit preparing the fire report |
| source_reporting_unit_name      | Code for the agency unit preparing the fire report, based on code/name in the source dataset |
| local_fire_report_id      | Number or code that uniquely identifies an incident report for a particular reporting unit and a particular calendar year |
| local_incident_id      | Number or code that uniquely identifies an incident for a particular local fire management organization within a particular calendar year |
|  fire_code     |  Code used within the interagency wildland fire community to track and compile cost information for emergency fire suppression |
| fire_name      | Name of the incident, from the fire report (primary) or ICS-209 report (secondary) |
| ics_209_incident_number      | Incident (event) identifier, from the ICS-209 report |
| ics_209_name      | Name of the incident, from the ICS-209 report |
| complex_name      | Name of the complex under which the fire was ultimately managed, when discernible |
| discovery_date      |Date on which the fire was discovered or confirmed to exist  |
| discovery_time      |  Time of day that the fire was discovered or confirmed to exist |
| stat_cause_descr      |Description of the (statistical) cause of the fire  |
|  cont_date     |  Date on which the fire was declared contained or otherwise controlled (mm/dd/yyyy where mm=month, dd=day, and yyyy=year) |
| cont_time      |Time of day that the fire was declared contained or otherwise controlled (hhmm where hh=hour, mm=minutes)  |
|fire_size      |Estimate of acres within the final perimeter of the fire  |
|  fire_size_class     |Code for fire size based on the number of acres within the final fire perimeter expenditures (A=greater than 0 but less than or equal to 0.25 acres, B=0.26-9.9 acres, C=10.0-99.9 acres, D=100-299 acres, E=300 to 999 acres, F=1000 to 4999 acres, and G=5000+ acres)  |
|  latitude     | Latitude (NAD83) for point location of the fire (decimal degrees) |
|  longitude     |  Longitude (NAD83) for point location of the fire (decimal degrees) |
|  geom     | Geometry coordinates column based on latitiude and longitude |
| owner_name      | Name of primary owner or entity responsible for managing the land at the point of origin of the fire at the time of the incident |
| us_state      | Two-letter alphabetic code for the state in which the fire burned (or originated), based on the nominal designation in the fire report |
| us_county      | County, or equivalent, in which the fire burned (or originated), based on nominal designation in the fire report |

**us_state**

| Column         | Description     |
|--------------|-----------|
| us_state      | Two-letter alphabetic code for US state |

**time_table**

| Column         | Description     |
|--------------|-----------|
| time      | Time in format HH24:MI |
| hour      | Int indicating the hour of the day |
| minute      | Int indicating the minute of the hour |

**date_table**

| Column         | Description     |
|--------------|-----------|
| date      | Date in format DD-MM-YYYY |
| day_of_year      | Int indicating the day of the year |
| month      | Int indicating the month of the year |
| weekday      | Int indicating the day of the week |
| week      | Int indicating the week of the year |
| year      | Int indicating the year |

**weather_stations**

| Column         | Description     |
|--------------|-----------|
| station_id      | Unique identifier of weather station |
| station_name      | Name of weather station |
| us_state      | State of the weather station |
| latitude      | Latitude (NAD83) for point location of the weather station |
| longitude      | Longitude (NAD83) for point location of the weather station |
| geom      | Geometry coordinates column based on latitiude and longitude |
| elevation      | Elevation of weather station above see-level (in meters) |

**weather_measurements**

| Column         | Description     |
|--------------|-----------|
| measurement_id      | Unique identifier of weather station |
| station_id      | Name of weather station |
| date      | State of the weather station |
| awnd | Average daily wind speed|
| evap |Evaporation of water from evaporation pan |
| fmtm |Time of fastest mile or fastest 1-minute wind|
| pgtm |Peak gust time (hours and minutes, i.e., HHMM) |
| prcp |Precipitation |
| sn52 | Minimum soil temperature with sod cover at 10 cm depth|
| sn53 | SN53,Minimum soil temperature with sod cover at 20 cm depth|
| sn55 | Minimum soil temperature with sod cover at 100 cm depth|
| snow | Snowfall (mm)|
| snwd | Snow depth (mm)|
| sx02 |Maximum soil temperature with unknown cover at 10 cm depth |
| sx52 |Maximum soil temperature with sod cover at 10 cm depth |
| sx53 |Maximum soil temperature with sod cover at 20 cm depth |
| sx55 | Maximum soil temperature with sod cover at 100 cm depth|
| tavg |Average temperature |
| tmax |Maximum temperature (tenths of degrees C) |
| tmin |Minimum temperature (tenths of degrees C) |
| tobs |Temperature at the time of observation (tenths of degrees C) |
| wdf1 |Direction of fastest 1-minute wind (degrees) |
| wdf2 |Direction of fastest 2-minute wind (degrees) |
| wdf5 |Direction of fastest 5-second wind (degrees) |
| wdmv |24-hour wind movement |
| wsf1 |Fastest 1-minute win  d speed |
| wsf2 |Fastest 2-minute wind speed |
| wsf5 |Fastest 5-second wind speed |
| wsfg |Peak guest wind speed|

**distance_table**

| Column         | Description     |
|--------------|-----------|
| wildfire_id      | Unique identifier of wildfire |
| station_id      | Unique identifier of weather station |
| distance      | Distance in meters between the corresponding wildfire and weather station |
