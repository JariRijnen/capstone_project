import argparse

from pyspark.sql import SparkSession


def pre_process_wildfire(spark, input_loc, output_loc):
    """
    Read csv file, drop "Shape" column and save as parquet.
    """

    # read input
    df_raw = spark.read.option("header", True).csv(input_loc)

    df_out = df_raw.drop("Shape")

    # parquet is a popular column storage format, we use it here
    df_out.repartition(1).write.mode("overwrite").option("header", True) \
        .option("compression", "uncompressed").parquet(output_loc)


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Pre-Process wildfire data") \
        .getOrCreate()

    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, help="csv input", default="/wildfires")
    parser.add_argument("--output", type=str, help="parquet output", default="/output")
    args = parser.parse_args()
    pre_process_wildfire(spark, input_loc=args.input, output_loc=args.output)
