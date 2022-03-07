## project capstone Wildfires

## Purpose
The purpose of this project is to investigate the relations between weather measurements and wildfires in the United States. E.g. one could investigate how much the rainfall in a previous month relates to the occurrence of wildfires, or could investigate how much the temperature influences the spread (size) of wildfires.

## data

* wildfires.parquet 

Wildfires data from [Kaggle](https://www.kaggle.com/rtatman/188-million-us-wildfires). Original is a table in a SQLite database. I saved it to .parquet and saved it on S3 in AWS.

It consists of 1.88 million wildfires that occurred in the US from 1992 to 2015.

* US_weather_all.csv

Weather data from [Kaggle](https://www.kaggle.com/cavfiumella/us-weather-daily-summaries-1991-2016). Original is 1001 different csv files. I have merged them into one .csv file and saved that on S3 in AWS.

It consists of daily weather summaries over 150 weather stations across the US from 1991 to 1996. In total there are 4,169,412 daily measurements. Not all stations measure the same statistics. For explanations about the variables, [definitions.csv](https://github.com/JariRijnen/capstone_project/definitions.csv).

## ER diagram

![ER-diagram](/images/ER-diagram.png)

The ER-diagram consists of two facts tables, wildfires and weather_measurements. There are four additional dimension tables: us_state, weather_stations, date_table, time_table.

## Airflow Data Pipeline

Run locally in Docker container. To start, make sure that Docker is running and execute 

```
docker-compose up
```

![airflow-pipeline](/images/airflow-pipeline.png)

The pipeline is locally executed via Airflow. The following steps are undertaken:
* Begin_execution: a dummy operator to start the pipeline.
* Resume_redshift_cluster: operator that resumes (if necessary) a Redshift cluster in AWS.
* drop_tables_if_exists: operator that drops redshift tables if exists.
* create_staging_tables_redshift: operator to create the redshift tables needed for staging the data. 
* stage_weather & stage_wildfire: two staging redshift queries that load the data from S3 into the staging tables.
* insert_fact_tables_redshift: operator to insert redshift data from the staging tables into the fact tabes.
* insert_dimension_tables: operator to insert data from the staging tables into the dimension tables.
* data_quality check: operator with two data quality checks for all of the final fact and dimension tables. 
* pause_redshift_cluster: operator to pause the used redshift cluster.
* stop_execution: dummy operator to close the pipeline.