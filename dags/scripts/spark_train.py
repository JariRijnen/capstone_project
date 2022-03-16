import os
from pyspark.sql import SparkSession


def csv_to_parquet(spark, input_loc, output_loc):
    """Spark functions to create parquet files from csv."""

    df_csv = spark.read.option("header", True).csv(input_loc)

    df_csv.write.mode("overwrite").parquet(output_loc)


def main():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()

    input_loc = os.environ['input_loc']
    output_loc = os.environ['output_loc']

    csv_to_parquet(spark, input_loc, output_loc)


if __name__ == "__main__":
    main()
